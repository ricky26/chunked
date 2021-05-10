//! A snapshot implementation which snapshots the state of a World.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::ops::Deref;
use std::sync::Arc;

use rayon::iter::{
    IntoParallelRefIterator,
    ParallelIterator,
};

use crate::{Archetype, ComponentTypeID};
use crate::archetype::{ComponentSetExt, ComponentVecSet};
use crate::chunk::{Chunk, ChunkAction, ChunkEdit, EntityEntry};
use crate::chunk_set::ChunkSet;
use crate::component_data::{ComponentDataVecWriter, ComponentValueRef};
use crate::entity::EntityID;
use crate::universe::Universe;

/// A single action as part of an edit list.
///
/// This can set or remove a single component.
#[derive(Clone, Debug)]
pub enum EditAction<'a> {
    SetComponent(ComponentValueRef<'a>),
    RemoveComponent(ComponentTypeID),
}

/// A single edit in an edit list.
#[derive(Clone, Debug)]
pub struct Edit<'a>(pub EntityID, pub EditAction<'a>);

/// A snapshot of the state of the world.
#[derive(Clone)]
pub struct Snapshot {
    universe: Arc<Universe>,
    chunk_sets: Vec<ChunkSet>,
    entities: BTreeMap<EntityID, usize>,
}

impl Snapshot {
    /// Create a new snapshot of an empty world.
    pub fn empty(universe: Arc<Universe>) -> Snapshot {
        Snapshot {
            universe,
            chunk_sets: Vec::new(),
            entities: BTreeMap::new(),
        }
    }

    /// Get a weak reference to the owning universe of this snapshot.
    pub fn universe(&self) -> &Arc<Universe> {
        &self.universe
    }

    /// Get all the `ChunkSet`s in this snapshot.
    pub fn chunk_sets(&self) -> &[ChunkSet] {
        &self.chunk_sets
    }

    /// Create a parallel iterator over the chunks in the snapshot.
    pub fn par_iter_chunk_sets(&self) -> impl ParallelIterator<Item=&ChunkSet> {
        self.chunk_sets.par_iter()
    }

    /// Create an iterator over all chunk sets.
    pub fn iter_chunk_sets(&self) -> impl Iterator<Item=&ChunkSet> {
        self.chunk_sets.iter()
    }

    /// Create a parallel iterator over the chunks in the snapshot.
    pub fn par_iter_chunks(&self) -> impl ParallelIterator<Item=&Arc<Chunk>> {
        self.chunk_sets.par_iter().flat_map(|chunks| chunks.par_iter())
    }

    /// Create an iterator over all the chunks in the snapshot.
    pub fn iter_chunks(&self) -> impl Iterator<Item=&Arc<Chunk>> {
        self.chunk_sets.iter().flat_map(|chunks| chunks.iter())
    }

    /// Get the `ChunkSet` for a particular archetype.
    pub fn chunk_set(&self, a: &Arc<Archetype>) -> Option<&ChunkSet> {
        self.chunk_sets.get(a.id())
    }

    /// Get a mutable reference to a `ChunkSet` for a particular archetype.
    pub fn chunk_set_mut(&mut self, a: &Arc<Archetype>) -> Option<&mut ChunkSet> {
        self.chunk_sets.get_mut(a.id())
    }

    /// Get an `EntityEntry` for the entity with the given ID.
    ///
    /// Returns None if the entity doesn't exist in this snapshot.
    pub fn entity(&self, id: EntityID) -> Option<EntityEntry> {
        self.entities.get(&id)
            .cloned()
            .and_then(|arch_idx| self.chunk_sets.get(arch_idx))
            .and_then(|chunks| chunks.chunk_for_entity(id))
            .and_then(|chunk| {
                let ids = chunk.components::<EntityID>().unwrap();
                ids.binary_search(&id).ok()
                    .and_then(|idx| chunk.entity_by_index(idx))
            })
    }

    /// Modify this snapshot, producing another snapshot, with the given edit list applied.
    ///
    /// If this is the only `Arc` to this snapshot, the memory will be reused. This is
    /// also true of the contained `ChunkSet`s and `Chunk`s.
    pub fn modify<'a, 'b, E>(this: impl Into<Modifiable<'b, Self>>, edits: E)
        where E: Iterator<Item=Edit<'a>>
    {
        let mut this = this.into();

        let edit_list = SnapshotEditList::from_edits(&mut this, edits);
        if !edit_list.is_empty() {
            let archetype_edits = edit_list.chunk_set_edits;

            let universe = this.universe.clone();
            let edit_snap = this.make_mut();
            if edit_snap.chunk_sets.len() < archetype_edits.len() {
                edit_snap.chunk_sets.resize(archetype_edits.len(), ChunkSet::new());
            }

            let chunk_sets = edit_snap.chunk_sets.iter_mut();
            let arch_edits = archetype_edits.into_iter();
            let arch_edit_sets = arch_edits.zip(chunk_sets)
                .enumerate()
                .map(|(id, (edits, chunk_set))|
                    (universe.archetype_by_id(id).unwrap(), edits, chunk_set));

            // TODO: make this parallel.
            for (arch, edits, chunk_set) in arch_edit_sets {
                chunk_set.modify(arch, edits, &edit_list.component_data);
            }
        }
    }
}

impl Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Snapshot {{")?;

        writeln!(f, "  Chunk Sets:")?;
        for (arch_id, chunk_set) in self.chunk_sets.iter().enumerate() {
            if chunk_set.is_empty() {
                continue;
            }

            let archetype = self.universe.archetype_by_id(arch_id).unwrap();

            write!(f, "    #{} - ", arch_id)?;
            for ty in archetype.component_types().as_slice() {
                write!(f, "{:?}, ", ty)?;
            }
            writeln!(f)?;

            writeln!(f, "      Chunks:")?;
            for chunk in chunk_set.iter() {
                writeln!(f, "        {:?} - {} entities", chunk.deref() as *const _, chunk.len())?;

                let ids = chunk.components::<EntityID>().unwrap();

                for entity_id in ids {
                    writeln!(f, "          Entity {:?} - Chunk {:?}", entity_id, chunk.deref() as *const _)?;
                }
            }
        }

        writeln!(f, "}}")
    }
}

/// An trait used to improve the ergonomics of `Snapshot::modify`.
pub trait ModifySnapshot<'a>: Into<Modifiable<'a, Snapshot>> {
    fn modify<'b, E: Iterator<Item=Edit<'b>>>(self, edits: E) {
        Snapshot::modify(self, edits)
    }
}

impl<'a, T: Into<Modifiable<'a, Snapshot>>> ModifySnapshot<'a> for T {}

/// A const reference to a `T` which can be promoted to a mutable one,
/// whilst similar to `Cow`, this also supports `Arc`s.
pub enum Modifiable<'a, T: Clone> {
    /// Use an `Arc` to promote with `Arc::make_mut`.
    Arc(&'a mut Arc<T>),
    /// Use a `Cow` to promote with `Cow::to_owned`.
    Cow(&'a mut Cow<'a, T>),
    /// Use a mutable reference, which does not need to be promoted.
    Ref(&'a mut T),
}

impl<'a, T: Clone> Modifiable<'a, T> {
    /// Promote this `Modifiable` to a mutable `T`, if it is not already,
    /// and return a mutable reference.
    pub fn make_mut(&mut self) -> &mut T {
        match self {
            Modifiable::Arc(x) => Arc::make_mut(x),
            Modifiable::Cow(x) => x.to_mut(),
            Modifiable::Ref(x) => x,
        }
    }
}

impl<'a, T: Clone> Deref for Modifiable<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Modifiable::Arc(x) => &**x,
            Modifiable::Cow(x) => &*x,
            Modifiable::Ref(x) => &*x,
        }
    }
}

impl<'a, T: Clone> From<&'a mut T> for Modifiable<'a, T> {
    fn from(x: &'a mut T) -> Self {
        Modifiable::Ref(x)
    }
}

impl <'a, T: Clone> From<&'a mut Arc<T>> for Modifiable<'a, T> {
    fn from(x: &'a mut Arc<T>) -> Self {
        Modifiable::Arc(x)
    }
}

impl<'a, T: Clone> From<&'a mut Cow<'a, T>> for Modifiable<'a, T> {
    fn from(x: &'a mut Cow<'a, T>) -> Self {
        Modifiable::Cow(x)
    }
}

struct SnapshotEditList<'a> {
    component_data: Vec<ComponentValueRef<'a>>,
    chunk_set_edits: Vec<Vec<ChunkEdit>>,
}

impl<'a> SnapshotEditList<'a> {
    fn get_chunk_set_edits<'b: 'c, 'c>(edits: &'b mut Vec<Vec<ChunkEdit>>, arch: &Arc<Archetype>) -> &'c mut Vec<ChunkEdit> {
        let id = arch.id();
        if edits.len() <= id {
            edits.resize(id + 1, Vec::new());
        }
        &mut edits[id]
    }

    pub fn from_edits<E>(snap: &mut Modifiable<'_, Snapshot>, edits: E) -> SnapshotEditList<'a>
        where E: Iterator<Item=Edit<'a>>
    {
        let mut component_data = Vec::new();
        let mut chunk_set_edits = Vec::new();
        let mut edits = edits.peekable();

        while let Some(Edit(id, _)) = edits.peek().cloned() {
            let old_archetype = snap.entities.get(&id)
                .copied()
                .and_then(|idx| snap.universe.archetype_by_id(idx));
            let mut component_types = match old_archetype {
                Some(ref a) => Cow::Borrowed(a.component_types()),
                None => Cow::Owned(ComponentVecSet::new(Vec::new())),
            };

            let mut component_data = ComponentDataVecWriter::new(&mut component_data);

            while edits.peek().map_or(false, |e| e.0 == id) {
                match edits.next().unwrap().1 {
                    EditAction::SetComponent(value) => {
                        if !component_types.includes(&value.type_id()) {
                            component_types.to_mut().insert(value.type_id());
                        }
                        component_data.set_component(value);
                    }
                    EditAction::RemoveComponent(type_id) => {
                        if component_types.includes(&type_id) {
                            component_types.to_mut().remove(type_id);
                        }
                        component_data.remove_component(type_id);
                    }
                }
            }

            let new_archetype = match component_types {
                Cow::Borrowed(_) => Some(old_archetype.clone().unwrap()),
                Cow::Owned(component_set) => {
                    if component_set.len() > 1 {
                        Some(snap.universe.ensure_archetype(component_set))
                    } else {
                        None
                    }
                }
            };

            let is_empty = old_archetype.is_none() && new_archetype.is_none();
            let is_move = old_archetype
                .clone()
                .and_then(|old| new_archetype.clone().map(|new| (old, new)))
                .map_or(true, |(a, b)| !Arc::ptr_eq(&a, &b));
            let is_noop = !is_empty && !is_move && component_data.is_empty();
            if is_noop {
                continue;
            }

            // Remove old entity.
            if let Some(arch) = old_archetype.filter(|_| is_move) {
                SnapshotEditList::get_chunk_set_edits(&mut chunk_set_edits, &arch)
                    .push(ChunkEdit(id, ChunkAction::Remove));

                let edit_snap = snap.make_mut();
                edit_snap.entities.remove(&id);
            }

            // Upsert entity.
            if let Some(arch) = new_archetype {
                let (start, end) = component_data.range();
                SnapshotEditList::get_chunk_set_edits(&mut chunk_set_edits, &arch)
                    .push(ChunkEdit(id, ChunkAction::Upsert(start, end)));

                if is_move {
                    let edit_snap = snap.make_mut();
                    edit_snap.entities.insert(id, arch.id());
                }
            }
        }

        SnapshotEditList {
            component_data,
            chunk_set_edits,
        }
    }

    /// Returns true if there are no changes associated with this edit list.
    pub fn is_empty(&self) -> bool {
        self.chunk_set_edits.is_empty()
    }
}

