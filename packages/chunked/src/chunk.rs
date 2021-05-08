//! Logic for dealing with chunks of entities.

use std::alloc::Layout;
use std::cmp::{Ord, Ordering};
use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::ptr::{self, NonNull};
use std::sync::Arc;

use bit_vec::BitVec;

use crate::{Archetype, EntityID};
use crate::archetype::ComponentVecSet;
use crate::component::{
    Component,
    ComponentTypeID,
};
use crate::component_data::{ComponentData, ComponentValueRef, ComponentDataSlice};
use std::convert::TryFrom;


/// A single action on an entity in a `Chunk`.
#[derive(Clone)]
pub(crate) enum ChunkAction {
    Upsert(usize, usize),
    Remove,
}

/// An edit list for one or more `Chunk`s.
#[derive(Clone)]
pub(crate) struct ChunkEdit(pub EntityID, pub ChunkAction);

/// A single `Chunk` of entities of the same `Archetype`.
/// 
/// The components are stored as a struct of arrays in one contiguous block of
/// memory.
pub struct Chunk {
    archetype: Arc<Archetype>,
    ptr: NonNull<u8>,
    len: usize,
}

unsafe impl Send for Chunk {}
unsafe impl Sync for Chunk {}

impl Chunk {
    /// Create a new chunk from the raw parts.
    pub unsafe fn from_raw(archetype: Arc<Archetype>, ptr: NonNull<u8>, len: usize) -> Chunk {
        Chunk {
            archetype,
            ptr,
            len,
        }
    }

    /// Return the `Archetype` this chunk belongs to.
    pub fn archetype(&self) -> &Arc<Archetype> {
        &self.archetype
    }

    /// Get the total number of entities currently stored in this chunk.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get the total capacity of this chunk (usually this is fixed).
    pub fn capacity(&self) -> usize {
        self.archetype.chunk_capacity()
    }

    /// Get the memory layout of this chunk.
    pub fn layout(&self) -> Layout {
        self.archetype.chunk_layout()
    }

    /// Get a slice of components from this chunk.
    pub fn components<T: Component>(&self) -> Option<&[T]> {
        self.archetype.component_offset(T::type_id()).map(|offset| {
            unsafe {
                let ptr = self.ptr.as_ptr().offset(offset as isize) as *const T;
                &*std::ptr::slice_from_raw_parts(ptr, self.len)
            }
        })
    }

    /// Get a mutable list of components from this chunk.
    pub fn components_mut<T: Component>(&mut self) -> Option<&mut [T]> {
        self.archetype.component_offset(T::type_id()).map(|offset| {
            unsafe {
                let ptr = self.ptr.as_ptr().offset(offset as isize) as *mut T;
                &mut *std::ptr::slice_from_raw_parts_mut(ptr, self.len)
            }
        })
    }

    /// Create a new `ChunkSplitter` for accessing multiple component lists at the
    /// same time.
    pub fn split_by_component_mut(&mut self) -> ChunkSplitter {
        ChunkSplitter::new(self)
    }

    /// Update the components of an entity in this chunk at the given index.
    pub fn update_at<'a>(&mut self, index: usize, component_data: &impl ComponentData<'a>) {
        self.apply_at(index, component_data, false)
    }

    /// Replace the entity at the given index.
    pub fn replace_at<'a>(&mut self, index: usize, component_data: &impl ComponentData<'a>) {
        self.apply_at(index, component_data, true)
    }

    /// Apply component data to an entity, optionally clearing it.
    fn apply_at<'a>(&mut self, index: usize, component_data: &impl ComponentData<'a>, clear: bool) {
        assert!(self.len > index, "Items can only be replaced at positions up to len()");
        let components = self.archetype.component_types().as_slice().iter()
            .zip(self.archetype.component_offsets().iter());
        let mut component_data = component_data.iter().peekable();

        for (ty, offset) in components {
            let data = component_data.peek();
            let cmp = data.map_or(Ordering::Greater, |v| v.type_id().cmp(ty));

            let src = match cmp {
                Ordering::Less => panic!("tried to update non-existing component"),
                Ordering::Equal => data,
                Ordering::Greater => {
                    if !clear {
                        continue;
                    }

                    None
                }
            };

            let layout = ty.layout();
            let size = layout.size();

            let item_offset = offset + (size * index);

            let dest_slice = {
                unsafe {
                    let ptr = self.ptr.as_ptr().offset(item_offset as isize);
                    &mut *std::ptr::slice_from_raw_parts_mut(ptr, size)
                }
            };

            if let Some(src) = src {
                let src_slice = src.as_slice();
                assert_eq!(src_slice.len(), dest_slice.len());
                dest_slice.copy_from_slice(src_slice);
                component_data.next();
            } else {
                ty.registration().set_default(dest_slice);
            }
        }
    }

    /// Insert an entity into this chunk, using the factory function provided to
    /// fill components.
    pub fn insert<'a>(&mut self, index: usize, component_data: &impl ComponentData<'a>) {
        assert!(self.len >= index, "Items can only be inserted at positions up to len()");

        let num_to_move = self.len - index;
        self.len += 1;
        let components = self.archetype.component_types().as_slice().iter()
            .zip(self.archetype.component_offsets().iter());
        let mut component_data = component_data.iter().peekable();

        for (ty, offset) in components {
            let data = component_data.peek();
            let cmp = data.map_or(Ordering::Greater, |v| v.type_id().cmp(ty));

            let src = match cmp {
                Ordering::Less => panic!("tried to insert component not in chunk"),
                Ordering::Equal => Some(data.unwrap()),
                Ordering::Greater => None,
            };

            let layout = ty.layout();
            let size = layout.size();
            let begin_offset = offset + (size * index);

            unsafe {
                if num_to_move > 0 {
                    let count = size * num_to_move;
                    let from = self.ptr.as_ptr().offset(begin_offset as isize);
                    let to = from.offset(size as isize);

                    ptr::copy(from, to, count);
                }

                let dest_slice = {
                    let ptr = self.ptr.as_ptr().offset(begin_offset as isize);
                    &mut *std::ptr::slice_from_raw_parts_mut(ptr, size)
                };

                if let Some(src_slice) = src.map(|v| v.as_slice()) {
                    dest_slice.copy_from_slice(src_slice);
                    component_data.next();
                } else {
                    ty.registration().set_default(dest_slice);
                }
            }
        }
    }

    /// Insert a number of entities from another chunk at the given index.
    pub fn copy_from(&mut self, insert_at: usize, other: &Chunk, range: impl RangeBounds<usize>) {
        assert!(std::ptr::eq(&*self.archetype, &*other.archetype), "chunks can only share elements in the same archetype");
        let src_start = match range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(x) => *x,
            Bound::Excluded(x) => 1 + *x,
        };
        let src_end = match range.end_bound() {
            Bound::Unbounded => self.len,
            Bound::Included(x) => 1 + *x,
            Bound::Excluded(x) => *x,
        };
        assert!(src_start <= src_end);
        assert!(src_start <= other.len);
        assert!(src_end <= other.len);

        let n = src_end - src_start;
        let dest_start = insert_at;
        let dest_end = insert_at + n;
        assert!(dest_start <= dest_end);
        assert!(dest_start <= self.len);
        assert!(dest_end <= self.len);

        let num_to_move = self.len - dest_start;
        self.len += n;
        let components = self.archetype.component_types().as_slice().iter()
            .zip(self.archetype.component_offsets().iter());

        for (ty, offset) in components {
            let layout = ty.layout();
            let size = layout.size();
            let insert_offset = offset + (size * dest_start);
            let source_offset = offset + (size * src_start);

            unsafe {
                if num_to_move > 0 {
                    let count = size * num_to_move;
                    let from = self.ptr.as_ptr().offset(insert_offset as isize);
                    let to = from.offset((n * size) as isize);
                    ptr::copy(from, to, count);
                }

                let write_ptr = self.ptr.as_ptr().offset(insert_offset as isize);
                let read_ptr = other.ptr.as_ptr().offset(source_offset as isize);
                std::ptr::copy_nonoverlapping(read_ptr, write_ptr, n);
            }
        }
    }

    /// Remove an entity from this chunk by its index into the chunk.
    pub fn remove(&mut self, range: impl RangeBounds<usize>) {
        let start = match range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(x) => *x,
            Bound::Excluded(x) => 1 + *x,
        };
        let end = match range.end_bound() {
            Bound::Unbounded => self.len,
            Bound::Included(x) => 1 + *x,
            Bound::Excluded(x) => *x,
        };
        assert!(start <= end);
        assert!(start <= self.len);
        assert!(end <= self.len);

        let num_to_move = self.len - end;
        self.len = start + num_to_move;
        let ptr = self.ptr.as_ptr();
        let components = self.archetype.component_types().as_slice().iter()
            .zip(self.archetype.component_offsets().iter());

        if num_to_move > 0 {
            for (ty, offset) in components {
                let layout = ty.layout();
                let size = layout.size();
                let dest_offset = (offset + (size * start)) as isize;
                let src_offset = (offset + (size * end)) as isize;
                let count = size * num_to_move;

                unsafe {
                    let to = ptr.offset(dest_offset);
                    let from = to.offset(src_offset);
                    ptr::copy(from, to, count);
                }
            }
        }
    }

    /// Get an entity entry by index.
    pub fn entity_by_index(self: &Arc<Chunk>, index: usize) -> Option<EntityEntry> {
        if self.len <= index {
            return None;
        }

        Some(EntityEntry {
            chunk: self.clone(),
            index,
        })
    }

    fn move_internal(&mut self, dest_idx: usize, src_idx: usize, n: usize) {
        if n == 0 || dest_idx == src_idx {
            return;
        }

        let components = self.archetype.component_types().as_slice().iter()
            .zip(self.archetype.component_offsets().iter());

        for (ty, offset) in components {
            let layout = ty.layout();
            let size = layout.size();
            let dest_offset = offset + (size * dest_idx);
            let src_offset = offset + (size * src_idx);

            unsafe {
                let write_ptr = self.ptr.as_ptr().offset(dest_offset as isize);
                let read_ptr = self.ptr.as_ptr().offset(src_offset as isize);
                std::ptr::copy(read_ptr, write_ptr, n * size);
            }
        }
    }

    /// Modify this chunk in-place with changes from an edit list.
    ///
    /// # Panics
    /// If `edits` is not in reverse order.
    pub(crate) fn modify<'a, I>(&mut self, edits: I, component_data: &[ComponentValueRef<'_>])
        where I: Iterator<Item=&'a ChunkEdit> + Clone
    {
        let mut read_idx = self.len();
        let mut write_idx = edits.clone().fold(self.len(), |acc, x| {
            let ChunkEdit(_, action) = x;

            match action {
                ChunkAction::Upsert(_, _) => acc + 1,
                ChunkAction::Remove => acc - 1,
            }
        });
        assert!(write_idx <= self.capacity());
        self.len = write_idx;

        for ChunkEdit(id, action) in edits.cloned() {
            let entity_ids = self.components::<EntityID>().unwrap();

            match action {
                ChunkAction::Upsert(data_start_idx, data_end_idx) => {
                    let data_slice = &component_data[data_start_idx..data_end_idx];
                    let data = ComponentDataSlice::try_from(data_slice).unwrap();

                    match entity_ids[..read_idx].binary_search(&id) {
                        Ok(idx) => {
                            self.update_at(idx, &data);
                        }
                        Err(idx) => {
                            let to_move = read_idx - idx;
                            read_idx = idx;
                            write_idx -= to_move + 1;
                            self.move_internal(write_idx + 1, read_idx, to_move);
                            self.replace_at(write_idx, &data);
                            self.components_mut::<EntityID>().unwrap()[write_idx] = id;
                        }
                    }
                }
                ChunkAction::Remove => {
                    let src_idx = entity_ids[..read_idx].binary_search(&id).unwrap();
                    let to_move = read_idx - (src_idx + 1);
                    self.move_internal(write_idx - to_move, read_idx - to_move, to_move);
                    read_idx = src_idx;
                    write_idx -= to_move;
                }
            }
        }
    }
}

impl Clone for Chunk {
    fn clone(&self) -> Self {
        let archetype = self.archetype.clone();
        let ptr = self.archetype.allocate_page();
        let layout = self.layout();

        if self.len > 0 {
            unsafe { std::ptr::copy(self.ptr.as_ptr(), ptr.as_ptr(), layout.size()) };
        }

        Chunk {
            archetype,
            ptr,
            len: self.len,
        }
    }
}

impl Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,
               "Chunk {{ archetype: {:?}, len: {} }}",
               self.archetype.as_ref() as *const _,
               self.len)
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        unsafe { self.archetype.free_page(self.ptr) };
    }
}

/// A utility for accessing multiple different component types from a 
/// chunk at the same time.
/// 
/// This is designed to be used when you need to read and write to multiple
/// component types in the same chunk at the same time.
/// 
/// At the moment, it is impossible to get two references to the same component
/// slice from `ChunkSplitter` mutable or not.
pub struct ChunkSplitter<'a> {
    chunk: &'a mut Chunk,
    locked: BitVec,
}

impl<'a> ChunkSplitter<'a> {
    /// Construct a new `ChunkSplitter` from a mutable `Chunk` reference.
    pub fn new(chunk: &mut Chunk) -> ChunkSplitter {
        let num_components = chunk.archetype().component_types().as_slice().len();

        ChunkSplitter {
            chunk,
            locked: BitVec::from_elem(num_components << 1, false),
        }
    }

    fn type_index(&self, type_id: &ComponentTypeID) -> Option<usize> {
        let types = self.chunk.archetype().component_types();
        types.as_slice()
            .binary_search(type_id)
            .ok()
    }

    fn mark_type(&mut self, type_id: &ComponentTypeID, mutable: bool) -> bool {
        let index = match self.type_index(type_id) {
            Some(x) => x,
            None => return false,
        };
        let offset = index << 1;
        let const_taken = self.locked[offset];
        let mut_taken = self.locked[offset + 1];

        if mutable && !mut_taken && !const_taken {
            self.locked.set(offset + 1, true);
            true
        } else if !mutable && !const_taken {
            self.locked.set(offset, true);
            true
        } else {
            false
        }
    }

    /// Get the slice of all components of the given type in the chunk.
    pub fn components<T: Component>(&mut self) -> Option<&'a [T]> {
        if self.mark_type(&T::type_id(), false) {
            let types = unsafe { std::mem::transmute(self.chunk.components::<T>().unwrap()) };
            Some(types)
        } else {
            None
        }
    }

    /// Get the mutable slice of all components of the given type in the chunk.
    pub fn components_mut<T: Component>(&mut self) -> Option<&'a mut [T]> {
        if self.mark_type(&T::type_id(), true) {
            let types = unsafe { std::mem::transmute(self.chunk.components_mut::<T>().unwrap()) };
            Some(types)
        } else {
            None
        }
    }
}

/// A reader for retrieving single entities from a snapshot.
pub struct EntityEntry {
    chunk: Arc<Chunk>,
    index: usize,
}

impl EntityEntry {
    /// Get the ID of the entity this reader refers to.
    pub fn entity_id(&self) -> EntityID {
        *self.component::<EntityID>().unwrap()
    }

    /// Return the archetype this Entity conforms to.
    pub fn archetype(&self) -> &Arc<Archetype> {
        self.chunk.archetype()
    }

    /// Get the component types attached to this entity.
    pub fn component_types(&self) -> &ComponentVecSet {
        self.archetype().component_types()
    }

    /// Get a reference to a component on the given entity.
    pub fn component<T: Component>(&self) -> Option<&T> {
        self.chunk.components::<T>()
            .and_then(|slice| slice.get(self.index))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ChunkEntityData {
    chunk: Arc<Chunk>,
    entity_index: usize,
    component_index: usize,
}

impl ChunkEntityData {
    /// Create a new `ChunkEntityData` for copying entities.
    pub fn new(chunk: Arc<Chunk>, entity_index: usize) -> ChunkEntityData {
        ChunkEntityData {
            chunk,
            entity_index,
            component_index: 0,
        }
    }
}

impl ComponentData<'static> for ChunkEntityData {
    type Iterator = ChunkEntityData;

    fn iter(&self) -> Self::Iterator {
        self.clone()
    }
}

impl Iterator for ChunkEntityData {
    type Item = ComponentValueRef<'static>;

    fn next(&mut self) -> Option<Self::Item> {
        let archetype = &self.chunk.archetype;
        let component_types = archetype.component_types();
        let component_types = component_types.as_slice();
        if self.component_index >= component_types.len() {
            return None;
        }

        let type_id = component_types[self.component_index];
        let size = type_id.layout().size();
        let offset = archetype.component_offset(type_id).unwrap()
            + (size * self.entity_index);

        let value = unsafe {
            let ptr = self.chunk.as_ref().ptr.as_ptr().offset(offset as isize);
            let slice = std::slice::from_raw_parts(ptr, size);
            ComponentValueRef::from_raw(type_id, slice)
        };
        self.component_index += 1;
        Some(value)
    }
}
