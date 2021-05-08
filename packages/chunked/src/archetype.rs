//! Archetypes are the 'layout' of entities, containing a list of the attached components.

use std::alloc;
use std::alloc::Layout;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::{Arc, Weak};
use std::sync::atomic::{self, AtomicUsize};

use crossbeam_queue::SegQueue;

use crate::{Component, ComponentTypeID, EntityID, Universe};
use crate::chunk::Chunk;

/// A list of component types which ensures a couple of useful invariants:
/// - Component Types are sorted
/// - EntityID is included
pub trait ComponentSet {
    /// Returns a sorted slice of the component types in the set.
    fn as_slice(&self) -> &[ComponentTypeID];

    /// Convert this set into an owned copy.
    ///
    /// If the set is already a `ComponentVecSet`, no allocation is performed.
    fn into_owned(self) -> ComponentVecSet;
}

pub trait ComponentSetExt: ComponentSet {
    /// Return the number of components in the component set.
    fn len(&self) -> usize { self.as_slice().len() }

    /// Return a borrowed version of this `ComponentSet`.
    fn as_ref(&self) -> ComponentSliceSet {
        ComponentSliceSet(self.as_slice())
    }

    /// Create an owned set from this set.
    ///
    /// This always creates a copy.
    fn to_owned(&self) -> ComponentVecSet {
        ComponentVecSet(self.as_slice().to_owned())
    }

    /// Returns true if this `ComponentSet` contains the given component.
    fn includes(&self, component_type: &ComponentTypeID) -> bool {
        self.as_slice().binary_search(component_type).is_ok()
    }

    /// Returns true if this `ComponentSet` contains all of the given component types.
    fn includes_all<T: Deref<Target=ComponentTypeID>>(&self, component_types: impl IntoIterator<Item=T>) -> bool {
        component_types.into_iter().all(|ct| self.includes(&*ct))
    }

    /// Returns true if these `ComponentSet`s contain the same types.
    fn eq(&self, other: &impl ComponentSet) -> bool {
        self.as_slice().eq(other.as_slice())
    }

    /// Return the ordering between this and another `ComponentSet`.
    fn cmp(&self, other: &impl ComponentSet) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl<T: ComponentSet + ?Sized> ComponentSetExt for T {}

fn ensure_component_set_valid(component_types: &mut Vec<ComponentTypeID>) {
    let entity_component_type = EntityID::type_id();

    component_types.dedup();
    if !component_types.contains(&entity_component_type) {
        component_types.push(entity_component_type);
    }
    component_types.sort();
}

/// A `Vec`-backed component set.
#[derive(Clone, Debug)]
pub struct ComponentVecSet(Vec<ComponentTypeID>);

impl ComponentVecSet {
    /// Create a new `ComponentVecSet` from a `Vec` of component types.
    pub fn new(mut component_types: Vec<ComponentTypeID>) -> ComponentVecSet {
        ensure_component_set_valid(&mut component_types);
        ComponentVecSet(component_types)
    }

    /// Return the slice contents of this `ComponentDataVec`.
    pub fn as_slice(&self) -> &[ComponentTypeID] {
        &self.0
    }

    /// Return the number of entries in this mapping.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Insert a component type into this set.
    pub fn insert(&mut self, component_type: ComponentTypeID) {
        match self.0.binary_search(&component_type) {
            Ok(_) => {}
            Err(idx) => self.0.insert(idx, component_type),
        }
    }

    /// Remove a component type from this set.
    pub fn remove(&mut self, component_type: ComponentTypeID) {
        match self.0.binary_search(&component_type) {
            Ok(idx) => { self.0.remove(idx); }
            Err(_) => {}
        };
    }
}

impl ComponentSet for ComponentVecSet {
    fn as_slice(&self) -> &[ComponentTypeID] {
        &self.0
    }

    fn into_owned(self) -> ComponentVecSet {
        self
    }
}

impl From<Vec<ComponentTypeID>> for ComponentVecSet {
    fn from(component_types: Vec<ComponentTypeID>) -> Self {
        ComponentVecSet::new(component_types)
    }
}

/// A slice-backed component set.
#[derive(Clone, Debug)]
pub struct ComponentSliceSet<'a>(&'a [ComponentTypeID]);

impl<'a> ComponentSliceSet<'a> {
    /// Create a new `ComponentSliceSet` by borrowing from a `Vec`.
    ///
    /// This takes a mutable reference as it makes the passed in `Vec` comply
    /// before taking a reference.
    pub fn ensure(component_types: &mut Vec<ComponentTypeID>) -> ComponentSliceSet {
        ensure_component_set_valid(component_types);
        ComponentSliceSet(component_types)
    }

    /// Try to create a `ComponentSet` from a slice, if it already meets the
    /// invariants.
    pub fn try_from_slice(component_types: &[ComponentTypeID]) -> Option<ComponentSliceSet> {
        let mut has_entity_id = false;
        let mut last: Option<ComponentTypeID> = None;

        for component_type in component_types.iter().cloned() {
            if let Some(l) = last {
                if l.cmp(&component_type) != Ordering::Less {
                    return None;
                }
            }

            last = Some(component_type.clone());
            if component_type == EntityID::type_id() {
                has_entity_id = true;
            }
        }

        if !has_entity_id {
            return None;
        }

        Some(ComponentSliceSet(component_types))
    }
}

impl<'a> ComponentSet for ComponentSliceSet<'a> {
    fn as_slice(&self) -> &[ComponentTypeID] {
        self.0
    }

    fn into_owned(self) -> ComponentVecSet {
        ComponentSetExt::to_owned(&self)
    }
}

impl<'a> TryFrom<&'a [ComponentTypeID]> for ComponentSliceSet<'a> {
    type Error = ();

    fn try_from(component_types: &'a [ComponentTypeID]) -> Result<Self, Self::Error> {
        ComponentSliceSet::try_from_slice(component_types).ok_or(())
    }
}

/// An archetype represents a particular layout of an entity.
///
/// It contains a set of `ComponentTypeID`s which are sorted for convenience.
pub struct Archetype {
    universe: Weak<Universe>,
    id: usize,
    component_types: ComponentVecSet,
    chunk_capacity: usize,
    chunk_layout: Layout,
    component_offsets: Vec<usize>,
    allocated_chunks: AtomicUsize,
    free_list: SegQueue<NonNull<u8>>,
}

unsafe impl Send for Archetype {}
unsafe impl Sync for Archetype {}

impl Archetype {
    /// Create a new archetype given the component set.
    pub(crate) fn new(universe: &Arc<Universe>, component_types: impl ComponentSet, id: usize) -> Archetype {
        let chunk_capacity = universe.chunk_size();
        let component_types = component_types.into_owned();
        let (component_offsets, chunk_layout) = Archetype::calculate_layout(&component_types, chunk_capacity);
        let universe = Arc::downgrade(&universe);

        Archetype {
            universe,
            id,
            component_types,
            chunk_capacity,
            chunk_layout,
            component_offsets,
            allocated_chunks: AtomicUsize::new(0),
            free_list: SegQueue::new(),
        }
    }

    /// Fetch the universe this archetype belongs to.
    pub fn universe(&self) -> &Weak<Universe> { &self.universe }

    /// Return the unique archetype ID for this universe.
    pub fn id(&self) -> usize { return self.id; }

    /// Return the sorted list of component types in this archetype.
    pub fn component_types(&self) -> &ComponentVecSet {
        &self.component_types
    }

    /// Get the maximum capacity of chunks in this `Zone`.
    pub fn chunk_capacity(&self) -> usize {
        self.chunk_capacity
    }

    /// Get the required memory layout for a chunk.
    pub fn chunk_layout(&self) -> Layout {
        self.chunk_layout
    }

    /// Returns true if this archetype contains the given component.
    pub fn has_component_type(&self, component_type: &ComponentTypeID) -> bool {
        self.component_types.includes(component_type)
    }

    /// Returns true if this archetype contains all of the given component types.
    pub fn has_all_component_types<T: Deref<Target=ComponentTypeID>>(&self, component_types: impl IntoIterator<Item=T>) -> bool {
        self.component_types.includes_all(component_types)
    }

    /// Returns the number of chunks currently allocated in this zone.
    ///
    /// This includes currently unused chunks in the free pool.
    pub fn allocated_chunks(&self) -> usize {
        self.allocated_chunks.load(atomic::Ordering::Relaxed)
    }

    /// Return an iterator over the component offsets of a Chunk.
    pub fn component_offsets(&self) -> &[usize] {
        &self.component_offsets
    }

    /// Get the offset into the chunk storage for a given component list.
    pub fn component_offset(&self, component_type: ComponentTypeID) -> Option<usize> {
        let component_offsets = self.component_offsets();
        self.component_types()
            .as_slice()
            .binary_search_by(|c| c.cmp(&component_type))
            .ok()
            .map(|idx| component_offsets[idx])
    }

    /// Create a new chunk with a given capacity.
    pub fn new_chunk(self: Arc<Self>) -> Chunk {
        let zone = self.clone();
        let ptr = self.allocate_page();
        unsafe { Chunk::from_raw(zone, ptr, 0) }
    }

    /// Deallocate all unused chunks.
    pub fn flush(&self) {
        while let Some(p) = self.free_list.pop() {
            unsafe { alloc::dealloc(p.as_ptr(), self.chunk_layout) };
            self.allocated_chunks.fetch_sub(1, atomic::Ordering::Relaxed);
        }
    }

    /// Allocate a new page for this Zone.
    pub fn allocate_page(&self) -> NonNull<u8> {
        if self.chunk_layout.size() == 0 {
            return NonNull::dangling();
        }

        match self.free_list.pop() {
            Some(ptr) => ptr,
            None => {
                self.allocated_chunks.fetch_add(1, atomic::Ordering::Relaxed);
                let raw_ptr = unsafe { alloc::alloc(self.chunk_layout) };
                NonNull::new(raw_ptr).unwrap()
            }
        }
    }

    /// Free a previously allocated page.
    pub unsafe fn free_page(&self, p: NonNull<u8>) {
        if self.chunk_capacity > 0 {
            self.free_list.push(p)
        }
    }

    /// Calculate the chunk layout of chunks in this zone.
    fn calculate_layout(component_types: &ComponentVecSet, capacity: usize) -> (Vec<usize>, Layout) {
        let mut offset = 0;
        let mut align = 1;

        let mut offsets = Vec::with_capacity(component_types.as_slice().len());

        for ty in component_types.as_slice().iter().cloned() {
            offsets.push(offset);

            let layout = ty.layout();
            let ty_align = layout.align();
            let size = layout.size();

            let misalignment = offset % align;
            if misalignment != 0 {
                offset += align - misalignment;
            }

            if ty_align > align {
                align = ty_align;
            }

            offset += capacity * size;
        }

        let layout = Layout::from_size_align(offset, align).unwrap();
        (offsets, layout)
    }
}

impl Drop for Archetype {
    fn drop(&mut self) {
        self.flush()
    }
}

/// Shortcut for neatly creating component sets.
#[macro_export]
macro_rules! component_set {
    () => { $crate::archetype::ComponentVecSet::new(Vec::new()) };
    ($x:expr, $($y:expr),*) => {
        $crate::archetype::ComponentVecSet::new(vec![$x, $($y),*])
    };
}
