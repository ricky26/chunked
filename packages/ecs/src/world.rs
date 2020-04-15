use std::ops::{Deref, DerefMut};
use std::fmt::{self, Debug};
use std::sync::{self, Arc, Weak, atomic::{self, AtomicUsize}};
use std::cmp::{Ord, Ordering};
use arc_swap::{ArcSwap};
use bit_vec::BitVec;

use crate::sorted_vec::{VecSet, VecMap};
use crate::entity::{
    Component,
    ComponentType,
    ComponentSource,
    Archetype,
    EntityID,
    EntityComponentIterator,
};
use crate::chunk::{Zone, Chunk, ChunkSet};

/// Return the first of the two options sorted by value.
fn option_min<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
    if a.is_none() && b.is_none() {
        None
    } else if a.is_none() {
        b
    } else if b.is_none() {
        a
    } else {
        let va = a.unwrap();
        let vb = b.unwrap();

        match va.cmp(&vb) {
            Ordering::Less | Ordering::Equal => Some(va),
            Ordering::Greater => Some(vb)
        }
    }
}

/// A writer for modifying snapshots.
/// 
/// This can be used in one of two ways:
/// - As a simple `Snapshot` reference.
/// - As a chunk-wise writer (which allows parallel writing).
pub struct SnapshotWriter {
    num_chunks: usize,
    contents: sync::Mutex<(Arc<Snapshot>, BitVec)>,
    cond: sync::Condvar,
}

impl SnapshotWriter {
    /// Create a new `SnapshotWriter` with the given starting state.
    pub fn new(snapshot: Arc<Snapshot>) -> SnapshotWriter {
        let num_chunks = snapshot.chunk_sets.iter()
            .map(|cs| cs.chunks().len())
            .sum();

        SnapshotWriter {
            num_chunks,
            contents: sync::Mutex::new((snapshot, BitVec::from_elem(num_chunks, false))),
            cond: sync::Condvar::new(),
        }
    }

    /// Deconstruct this writer and return the resultant snapshot.
    pub fn into_inner(self) -> Arc<Snapshot> {
        self.contents.into_inner().unwrap().0
    }

    /// Return the number of chunks covered by this writer.
    pub fn num_chunks(&self) -> usize {
        self.num_chunks
    }

    /// Set the final snapshot directly.
    pub fn set_snapshot(&self, snapshot: Arc<Snapshot>) {
        let mut contents = self.contents.lock().unwrap();
        contents.0 = snapshot;
    }

    /// Borrow a single chunk mutably.
    /// 
    /// This method cannot be used if `set_snapshot` has been called.
    pub fn borrow_chunk_mut(&self, index: usize) -> SnapshotWriterGuard {
        let (chunk_set, chunk_index) = loop {
            let mut contents = self.contents.lock().unwrap();

            let (chunk_set_index, chunk_index) = {
                let mut chunk_index = index;
                let mut chunk_set_index = 0;

                for chunk_set in contents.0.chunk_sets.iter() {
                    let num_chunks = chunk_set.chunks().len();
                    if num_chunks <= chunk_index {
                        chunk_set_index += 1;
                        chunk_index -= num_chunks;
                    } else {
                        break;
                    }
                }

                (chunk_set_index, chunk_index)
            };

            let index = contents.0.chunk_sets.iter()
                .map(|cs| cs.chunks().len())
                .take(chunk_set_index)
                .sum::<usize>() + chunk_index;

            if contents.1[index] {
                let _ = self.cond.wait(contents).unwrap();
            } else {
                contents.1.set(index, true);
                let snapshot_mut = Arc::make_mut(&mut contents.0);
                let chunk_set_mut = &mut snapshot_mut.chunk_sets[chunk_set_index];

                // This is safe since we only allow write access and only to
                // one person at a time.
                let chunk_set_mut: &mut ChunkSet = unsafe { std::mem::transmute(chunk_set_mut) };
                break (chunk_set_mut, chunk_index);
            }
        };

        let chunk = Arc::make_mut(chunk_set.chunk_mut(chunk_index).unwrap());
        SnapshotWriterGuard {
            writer: self,
            chunk,
            index,
        }
    }

    /// Free a chunk previously locked by `borrow_chunk_mut`.
    fn free_chunk(&self, index: usize) {
        let mut contents = self.contents.lock().unwrap();
        contents.1.set(index, false);
        self.cond.notify_all();
    }
}

// A guard which ensures that written chunks are released.
pub struct SnapshotWriterGuard<'a> {
    writer: &'a SnapshotWriter,
    chunk: &'a mut Chunk,
    index: usize,
}

impl<'a> Drop for SnapshotWriterGuard<'a> {
    fn drop(&mut self) {
        self.writer.free_chunk(self.index);
    }
}

impl<'a> Deref for SnapshotWriterGuard<'a> {
    type Target = Chunk;

    fn deref(&self) -> &Self::Target {
        self.chunk
    }
}

impl<'a> DerefMut for SnapshotWriterGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.chunk
    }
}

struct WorldGlobal {
    archetypes: ArcSwap<VecSet<Arc<Archetype>>>,
    zones: ArcSwap<Vec<Arc<Zone>>>,

    next_entity_id: AtomicUsize,
    next_zone_id: AtomicUsize,
    max_chunk_size: usize,
}

/// A World wraps up a set of `ChunkSet`s.
/// 
/// The `World` is the most common way of managing entities. You can create an
/// empty world by calling `World::new()`.
/// 
/// Worlds are capable of being updated via both `CommandBuffer`s and
/// `SystemSet`s which provide powerful ways of working with the contained
/// entities.
pub struct World {    
    global: Arc<WorldGlobal>,
    current_snapshot: ArcSwap<Snapshot>,
}

impl World {
    /// Create a new world with the default settings.
    pub fn new() -> World {
        World::with_max_chunk_size(4096)
    }

    /// Create a new world with a specified maximum number of entities per chunk.
    pub fn with_max_chunk_size(max_chunk_size: usize) -> World {
        let global = Arc::new(WorldGlobal{
            archetypes: ArcSwap::from_pointee(VecSet::new()),
            zones: ArcSwap::from_pointee(Vec::new()),

            next_zone_id: AtomicUsize::new(1),
            next_entity_id: AtomicUsize::new(1),
            max_chunk_size: max_chunk_size,
        });
        let current_snapshot = Snapshot::empty_for_global(Arc::downgrade(&global));

        World {
            global,
            current_snapshot: ArcSwap::from_pointee(current_snapshot),
        }
    }

    /// Create a snapshot of the current world state.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.current_snapshot.load_full()
    }

    /// Set the current state of the world.
    pub fn set_snapshot(&self, snapshot: Arc<Snapshot>) {
        assert!(snapshot.global().ptr_eq(&Arc::downgrade(&self.global)),
            "snapshot is not of this world");
        self.current_snapshot.store(snapshot);
    }

    /// Clear all entities from the world.
    pub fn clear(&self) {
        let snapshot = Arc::new(Snapshot::empty_for_global(Arc::downgrade(&self.global)));
        self.current_snapshot.store(snapshot);
    }

    /// Get a archetype with the given component types, providing it already
    /// exists in the world.
    /// 
    /// Generally, one should prefer `ensure_archetype` unless you have a
    /// particular reason to not want the archetype to be created, as it handles
    /// the fast case of the archetype already existing.
    pub fn get_archetype(&self, component_types: &mut Vec<ComponentType>) -> Option<Arc<Archetype>> {
        let entity_component_type = ComponentType::for_type::<EntityID>();

        component_types.dedup();
        if !component_types.contains(&entity_component_type) {
            component_types.push(entity_component_type);
        }
        component_types.sort();

        let archetypes = self.global.archetypes.load();
        archetypes.binary_search_by_key(&&component_types[..], |archetype| &*archetype.component_types())
            .ok()
            .map(|idx| archetypes[idx].clone())
    }

    /// Get the archetype which contains exactly the requested component types.
    /// 
    /// The archetype might not contain the given types in the order requested,
    /// they will be sorted by their unique ID.
    pub fn ensure_archetype(&self, mut component_types: Vec<ComponentType>) -> Arc<Archetype> {
        // Fast path for existing archetypes which are committed before this
        // function is entered.
        if let Some(existing_archetype) = self.get_archetype(&mut component_types) {
            return existing_archetype;
        }

        // Slow path which ensures no duplicate archetypes.
        let archetypes = &self.global.archetypes;
        let new_archetype = Arc::new(Archetype::new(component_types));

        loop {
            let previous = archetypes.load();

            let existing = previous.binary_search(&new_archetype)
                .ok()
                .map(|idx| previous[idx].clone());
            if let Some(existing) = existing {
                return existing;
            }

            let next_vec = previous.with(new_archetype.clone());
            let new_value = archetypes.compare_and_swap(&previous, Arc::new(next_vec));

            if std::ptr::eq(&*previous, &*new_value) {
                return new_archetype;
            }
        }
    }

    /// Ensure that a zone exists for the given archetype.
    fn ensure_zone(&self, archetype: &Arc<Archetype>) -> Arc<Zone> {
        let mut new_zone = None;

        loop {
            let zones = self.global.zones.load();

            return match zones.binary_search_by_key(&(&*archetype as &Archetype as *const Archetype), |z| &*z.archetype() as &Archetype as *const Archetype) {
                Ok(index) => {
                    zones[index].clone()
                },
                Err(index) => {
                    if new_zone.is_none() {
                        let zone_id = self.global.next_zone_id.fetch_add(1, atomic::Ordering::Relaxed);
                        new_zone = Some(Arc::new(Zone::new(archetype.clone(), self.global.max_chunk_size, zone_id)));
                    }

                    let mut new_zones = Vec::with_capacity(zones.len() + 1);
                    new_zones.extend(zones[..index].iter().cloned());
                    new_zones.push(new_zone.clone().unwrap());
                    new_zones.extend(zones[index..].iter().cloned());

                    let next_value = self.global.zones.compare_and_swap(&zones, Arc::new(new_zones));
                    if !std::ptr::eq(&*zones as &Vec<_>, &*next_value as &Vec<_>) {
                        continue;
                    }

                    new_zone.unwrap()
                },
            };
        }
    }

    /// Generate a new entity ID which is unique for this world.
    fn generate_entity_id(&self) -> EntityID {
        EntityID::new(self.global.next_entity_id.fetch_add(1, atomic::Ordering::Relaxed))
    }
}

impl Debug for World {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "World {{\n")?;
        write!(f, "  Zones:\n")?;

        for zone in self.global.zones.load().iter() {
            let archetype = &zone.archetype();

            write!(f, "    #{} - ", zone.key())?;
            for ty in archetype.component_types().iter() {
                write!(f, "{:?}, ", ty.type_id())?;
            }
            write!(f, "\n")?;
        }

        write!(f, "  Current Snapshot: {:?}\n", self.current_snapshot.load())?;
        write!(f, "}}\n")
    }
}

/// A snapshot of the state of the world.
#[derive(Clone)]
pub struct Snapshot {
    global: Weak<WorldGlobal>,
    chunk_sets: Vec<ChunkSet>,
}

impl Snapshot {
    /// Create a new snapshot of an empty world.
    pub fn empty(world: &Arc<World>) -> Snapshot {
        Snapshot::empty_for_global(Arc::downgrade(&world.global))
    }

    /// Create a new empty snapshot given the `WorldGlobal`.
    fn empty_for_global(global: Weak<WorldGlobal>) -> Snapshot {
        Snapshot {
            global,
            chunk_sets: Vec::new(),
        }
    }

    /// Get a weak reference to the owning global of this snapshot.
    fn global(&self) -> &Weak<WorldGlobal> {
        &self.global
    }

    /// Create an iterator over all the chunks in the snapshot.
    pub fn iter_chunks(&self) -> impl Iterator<Item=&Arc<Chunk>> {
        self.chunk_sets.iter().flat_map(|cs| cs.chunks())
    }

    /// Get an `EntityReader` for the entity with the given ID.
    /// 
    /// Returns None if the entity doesn't exist in this snapshot.
    pub fn entity_reader(self: &Arc<Self>, id: EntityID) -> Option<EntityReader> {
        self.chunk_sets.iter().enumerate()
            .filter_map(|(chunk_set_index, cs)| {
                if let Some((chunk_index, entity_index)) = cs.binary_search(id).ok() {
                    Some(EntityReader{
                        snapshot: self.clone(),
                        chunk_set_index,
                        chunk_index,
                        entity_index,
                    })
                } else {
                    None
                }
            })
            .next()
    }
}

impl Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Snapshot {{\n")?;

        write!(f, "  Chunks:\n")?;
        for chunk_set in self.chunk_sets.iter() {
            for chunk in chunk_set.chunks().iter() {
                write!(f, "    {:?} - zone {} - {} entities\n", chunk.deref() as *const _, chunk.zone().key(), chunk.len())?;
            }
        }

        write!(f, "  Entities:\n")?;        
        for chunk_set in self.chunk_sets.iter() {
            for chunk in chunk_set.chunks().iter() {
                let ids = chunk.get_components::<EntityID>().unwrap();

                for entity_id in ids {
                    write!(f, "    Entity {:?} - Chunk {:?}\n", entity_id, chunk.deref() as *const _)?;
                }
            }
        }

        write!(f, "}}\n")
    }
}

/// A reader for retreiving single entities from a snapshot.
pub struct EntityReader {
    snapshot: Arc<Snapshot>,
    chunk_set_index: usize,
    chunk_index: usize,
    entity_index: usize,
}

impl EntityReader {
    /// Get the ID of the entity this reader refers to.
    pub fn entity_id(&self) -> EntityID {
        *self.get_component::<EntityID>().unwrap()
    }

    /// Return the archetype this Entity conforms to.
    pub fn archetype(&self) -> &Arc<Archetype> {
        self.snapshot.chunk_sets[self.chunk_set_index].zone().archetype()
    }

    /// Create an iterator for the components on this entity.
    pub fn component_types(&self) -> EntityComponentIterator {
        EntityComponentIterator::for_archetype(self.archetype().clone())
    }

    /// Get a reference to a component on the given entity.
    pub fn get_component<T: Component>(&self) -> Option<&T> {
        let chunk_set = &self.snapshot.chunk_sets[self.chunk_set_index];
        let chunk = &chunk_set.chunks()[self.chunk_index];
        
        chunk.get_components::<T>()
            .and_then(|slice| slice.get(self.entity_index))
    }
}

/// A `ComponentSource` which is used by `CommandBuffer`s.
struct CommandBufferComponentSource<'a> {
    entity_id: EntityID,
    updates: &'a [((EntityID, ComponentType), (usize, usize))],
    component_storage: &'a [u8],
}

unsafe impl ComponentSource for CommandBufferComponentSource<'_> {
    fn set_component(&self, component_type: &ComponentType, storage: &mut [u8]) {
        if component_type.type_id() == EntityID::type_id() {
            let entity_id_bytes = unsafe {
                let ptr = &self.entity_id as *const _ as *const u8;
                std::slice::from_raw_parts(ptr, std::mem::size_of::<EntityID>())
            };
            
            storage.copy_from_slice(entity_id_bytes);
        } else if let Some(idx) = self.updates.binary_search_by_key(component_type, |update| (update.0).1).ok() {
            let (_, (offset, len)) = self.updates[idx];
            assert_eq!(storage.len(), len, "entity component size mismatch");
            storage.copy_from_slice(&self.component_storage[offset..offset + len]);
        } else {
            component_type.set_default(storage);
        }
    }
}

/// A command buffer for queuing up changes to multiple entities in a single
/// world.
pub struct CommandBuffer {
    world: Arc<World>,
    move_entities: VecMap<EntityID, Option<Arc<Archetype>>>,
    update_entities: VecMap<(EntityID, ComponentType), (usize, usize)>,
    component_storage: Vec<u8>,
}

impl CommandBuffer {
    /// Create a new CommandBuffer given the internal world contents.
    pub fn new(world: Arc<World>) -> CommandBuffer {
        CommandBuffer{
            world,
            move_entities: VecMap::new(),
            update_entities: VecMap::new(),
            component_storage: Vec::new(),
        }
    }

    /// Get the world this command buffer is operating on.
    pub fn world(&self) -> &Arc<World> {
        &self.world
    }

    /// Schedule a new entity for addition, returning the new entity ID immediately.
    /// 
    /// Whilst the ID is assigned, the entity won't exist in the world until
    /// the command buffer is executed. Really it's there for use in other commands.
    pub fn new_entity(&mut self, archetype: Arc<Archetype>) -> EntityID {
        let entity_id = self.world.generate_entity_id();
        self.move_entities.insert(entity_id, Some(archetype));
        entity_id
    }

    /// Destroy an existing entity in the world (or queued up for creation in
    /// this command buffer).
    pub fn destroy_entity(&mut self, entity_id: EntityID) {
        self.move_entities.insert(entity_id, None);
    }

    /// Update the contents of a component for an entity.
    /// 
    /// This function takes the raw bytes of the component type and will
    /// replace the component with them.
    pub unsafe fn set_component_raw(&mut self, entity_id: EntityID, component_type: ComponentType, contents: &[u8]) {
        let offset = self.component_storage.len();
        let len = contents.len();
        self.component_storage.extend(contents.iter());
        self.update_entities.insert((entity_id, component_type), (offset, len));
    }

    /// Update the contents of a component for an entity.
    pub fn set_component<T: Component>(&mut self, entity_id: EntityID, component: T) {
        let component_type = ComponentType::for_type::<T>();
        
        unsafe {
            let contents = {
                let ptr = &component as *const T as *const u8;
                let len = std::mem::size_of::<T>();
                std::slice::from_raw_parts(ptr, len)
            };

            self.set_component_raw(entity_id, component_type, contents);
        }
    }

    /// Create a new snapshot which is the result of applying this command
    /// buffer to a given snapshot.
    pub fn mutate_snapshot(&self, mut snapshot: Arc<Snapshot>) -> Arc<Snapshot> {
        let world = &self.world;
        let archetypes = world.global.archetypes.load();

        let move_entities = &self.move_entities;
        let update_entities = &self.update_entities;
        let component_storage = &self.component_storage;

        // TODO: This is a prime opportunity for parellelism.
        for chunk_set_index in 0..snapshot.chunk_sets.len() {
            let mut chunk_set = &snapshot.chunk_sets[chunk_set_index];
            let zone = chunk_set.zone();
            let arch_idx = archetypes.binary_search(zone.archetype()).ok();

            let mut chunk_index = 0;
            let mut index = 0;

            let mut move_index = 0;
            let mut update_index = 0;

            while chunk_index <= chunk_set.chunks().len() {
                let chunk = chunk_set.chunks().get(chunk_index);
                let entity_ids: &[EntityID] = chunk.map_or(&[], |c| c.get_components::<EntityID>().unwrap());

                let next_move_id = move_entities.get(move_index).map(|x| x.0);
                let next_update_id = update_entities.get(update_index).map(|x| (x.0).0);
                let next_entity_id = match option_min(next_move_id, next_update_id) {
                    Some(x) => x,
                    // No more modifications, just break out.
                    None => break,
                };

                let (exists, next_index) = match entity_ids[index..].binary_search(&next_entity_id) {
                    Ok(idx) => (true, idx + index),
                    Err(idx) => (false, idx + index),
                };
                index = next_index;

                while update_entities.get(update_index).map_or(false, |x| (x.0).0 < next_entity_id) {
                    update_index += 1;
                }
                let start_update_index = update_index;
                while update_entities.get(update_index).map_or(false, |x| (x.0).0 == next_entity_id) {
                    update_index += 1;
                }

                let updates = &update_entities[start_update_index..update_index];
                let component_source = CommandBufferComponentSource{
                    entity_id: next_entity_id,
                    updates,
                    component_storage: &component_storage,
                };

                let next_move = move_entities.get(move_index)
                    .filter(|x| x.0 == next_entity_id)
                    .map(|(_, arch)| arch.as_ref().map_or(false, |a| archetypes.binary_search(&a).ok() == arch_idx));

                if let Some(this_zone) = next_move {
                    move_index += 1;

                    if !exists && this_zone {
                        // Want to exist in this zone, but missing!
                        let snapshot_mut = Arc::make_mut(&mut snapshot);
                        let chunk_set_mut = &mut snapshot_mut.chunk_sets[chunk_set_index];
                        let (new_chunk_index, new_index) = chunk_set_mut.insert_at(chunk_index, index, &component_source);

                        chunk_set = &snapshot.chunk_sets[chunk_set_index];
                        chunk_index = new_chunk_index;
                        index = new_index;
                        continue;
                    } else if exists && !this_zone {
                        // Being removed from this zone.
                        let snapshot_mut = Arc::make_mut(&mut snapshot);
                        let chunk_set_mut = &mut snapshot_mut.chunk_sets[chunk_set_index];
                        let (new_chunk_index, new_index) = chunk_set_mut.remove_at(chunk_index, index);

                        chunk_set = &snapshot.chunk_sets[chunk_set_index];
                        chunk_index = new_chunk_index;
                        index = new_index;
                        continue;
                    }
                }

                if exists {
                    // Check for update!
                    let snapshot_mut = Arc::make_mut(&mut snapshot);
                    let chunk_set_mut = &mut snapshot_mut.chunk_sets[chunk_set_index];
                    let chunk_mut = Arc::make_mut(chunk_set_mut.chunk_mut(chunk_index).unwrap());
                    chunk_mut.replace_at(index, &component_source);
                    chunk_set = &snapshot.chunk_sets[chunk_set_index];
                    index += 1;
                } else {
                    chunk_index += 1;
                    index = 0;
                }
            }
        }

        {
            // Build new ChunkSets.
            let mut new_chunk_sets: Vec<ChunkSet> = Vec::new();
            let mut update_index = 0;

            for (entity_id, archetype) in move_entities.iter() {
                let archetype = match archetype {
                    Some(x) => x,
                    None => continue,
                };

                let zone = world.ensure_zone(archetype);

                if snapshot.chunk_sets.binary_search_by_key(&zone.key(), |cs| cs.zone().key()).is_ok() {
                    continue;
                }

                while update_entities.get(update_index).map_or(false, |((id, _), _)| id < entity_id) {
                    update_index += 1;
                }
                let start_index = update_index;
                while update_entities.get(update_index).map(|x| (x.0).0) == Some(*entity_id) {
                    update_index += 1;
                }

                let updates = &update_entities[start_index..update_index];
                let component_source = CommandBufferComponentSource{
                    entity_id: *entity_id,
                    updates,
                    component_storage: &component_storage,
                };

                match new_chunk_sets.binary_search_by_key(&zone.key(), |cs| cs.zone().key()) {
                    Ok(index) => {
                        let chunk_set = &mut new_chunk_sets[index];
                        chunk_set.push(&component_source);
                    },
                    Err(index) => {
                        let mut new_chunk_set = ChunkSet::new(zone.clone());
                        new_chunk_set.push(&component_source);
                        new_chunk_sets.insert(index, new_chunk_set);
                    },
                }
            }

            // Merge chunksets.
            if new_chunk_sets.len() > 0 {
                let snapshot_mut = Arc::make_mut(&mut snapshot);

                let mut merged_chunk_sets = Vec::with_capacity(snapshot_mut.chunk_sets.len() + new_chunk_sets.len());
                let mut old_iter = snapshot_mut.chunk_sets.drain(..).peekable();
                let mut new_iter = new_chunk_sets.into_iter().peekable();

                while old_iter.peek().is_some() || new_iter.peek().is_some() {
                    let old_v = old_iter.peek();
                    let new_v = new_iter.peek();

                    let is_new = if old_v.is_none() {
                        true
                    } else if new_v.is_none() {
                        false
                    } else {
                        let old_v = old_v.unwrap();
                        let new_v = new_v.unwrap();
                        old_v.zone().key() > new_v.zone().key()
                    };
                    
                    if is_new {
                        merged_chunk_sets.push(new_iter.next().unwrap());
                    } else {
                        merged_chunk_sets.push(old_iter.next().unwrap());
                    }
                }

                drop(old_iter);
                snapshot_mut.chunk_sets = merged_chunk_sets;
            }

            snapshot
        }
    }

    /// Consume the command buffer and apply the changes to the world.
    /// 
    /// This function may take some time as it has to acquire a lock on the world.
    pub async fn execute(self) {
        let world = &self.world;
        world.current_snapshot.store(self.mutate_snapshot(world.snapshot()));
    }
}
