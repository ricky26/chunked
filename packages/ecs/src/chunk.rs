//! Logic for dealing with chunks of entities.

use std::sync::Arc;
use std::fmt::Debug;
use std::alloc::{self, Layout};
use std::ptr::{self, NonNull};
use std::cmp::{Ord, Ordering};
use bit_vec::BitVec;
use crossbeam_queue::{ArrayQueue, PushError};

use crate::entity::{
    Component,
    ComponentTypeID,
    ComponentSource,
    Archetype,
    EntityID,
};

/// An iterator for the offsets of components stored in a chunk.
pub struct ComponentOffsetIterator {
    archetype: Arc<Archetype>,
    capacity: usize,
    type_index: usize,
    offset: usize,
    align: usize,
}

impl ComponentOffsetIterator {
    /// Create a new `ComponentOffsetIterator` for the given `Archetype` and
    /// chunk capacity.
    pub fn new(archetype: Arc<Archetype>, capacity: usize) -> ComponentOffsetIterator {
        ComponentOffsetIterator {
            archetype,
            capacity,
            type_index: 0,
            offset: 0,
            align: 0,
        }
    }

    /// Consume this iterator and return the layout of the entire component set.
    pub fn into_layout(mut self) -> Layout {
        while let Some(_) = self.next() {}
        Layout::from_size_align(self.offset.max(1), self.align).unwrap()
    }
}

impl Iterator for ComponentOffsetIterator {
    type Item = usize;
 
    fn next(&mut self) -> Option<usize> {
        let component_types = self.archetype.component_types();
        if self.type_index >= component_types.len() {
            None
        } else {
            let ty = component_types[self.type_index];
            self.type_index += 1;
            let offset = self.offset;

            let layout = ty.layout();
            let align = layout.align();
            let size = layout.size();

            let misalignment = self.offset % align;

            if misalignment != 0 {
                self.offset += align - misalignment;
            }

            if align > self.align {
                self.align = align;
            }

            self.offset += self.capacity * size;
            Some(offset)
        }
    }
}

/// A zone from which `Chunk`s can be allocated.
/// 
/// Largely `Zone` exists to optimise allocations performed for `Chunk`s.
pub struct Zone {
    archetype: Arc<Archetype>,
    capacity: usize,
    key: usize,
    chunk_layout: Layout,
    free_list: ArrayQueue<NonNull<u8>>,
}

unsafe impl Send for Zone {}
unsafe impl Sync for Zone {}

impl Zone {
    /// Construct a new Zone given the archetype and Zone ID.
    pub fn new(archetype: Arc<Archetype>, capacity: usize, key: usize) -> Zone {
        let chunk_layout = ComponentOffsetIterator::new(archetype.clone(), capacity).into_layout();

        Zone {
            archetype,
            capacity,
            key,
            chunk_layout,
            free_list: ArrayQueue::new(256),
        }
    }

    /// Get the sorting key for this `Zone`.
    pub fn key(&self) -> usize {
        self.key
    }

    /// Return the archetype this `Zone` covers.
    pub fn archetype(&self) -> &Arc<Archetype> {
        &self.archetype
    }

    /// Get the maximum capacity of chunks in this `Zone`.
    pub fn chunk_capacity(&self) -> usize {
        self.capacity
    }

    /// Get the required memory layout for a chunk.
    pub fn chunk_layout(&self) -> Layout {
        self.chunk_layout
    }

    /// Return an iterator over the component offsets of a Chunk.
    pub fn component_offset_iter(&self) -> ComponentOffsetIterator {
        ComponentOffsetIterator::new(self.archetype.clone(), self.capacity)
    }

    /// Create a new chunk with a given capacity.
    pub fn new_chunk(self: Arc<Self>) -> Chunk {
        let zone = self.clone();
        let ptr = self.allocate_page();

        Chunk {
            zone,
            ptr,
            len: 0,
        }
    }

    /// Allocate a new page for this Zone.
    pub fn allocate_page(&self) -> NonNull<u8> {
        if self.capacity > 0 {
            match self.free_list.pop() {
                Ok(ptr) => ptr,
                Err(_) => {
                    let raw_ptr = unsafe { alloc::alloc(self.chunk_layout) };
                    NonNull::new(raw_ptr).unwrap()
                },
            }
        } else {
            NonNull::dangling()
        }
    }

    /// Free a previously allocated page.
    pub unsafe fn free_page(&self, p: NonNull<u8>) {
        if self.capacity > 0 {
            match self.free_list.push(p) {
                Ok(()) => {},
                Err(PushError(p)) => {
                    alloc::dealloc(p.as_ptr(), self.chunk_layout);
                },
            }
        }
    }
}

impl Drop for Zone {
    fn drop(&mut self) {
        while let Ok(p) = self.free_list.pop() {
            unsafe { alloc::dealloc(p.as_ptr(), self.chunk_layout) };
        }
    }
}

/// A single `Chunk` of entities of the same `Archetype`.
/// 
/// The components are stored as a struct of arrays in one contiguous block of
/// memory.
pub struct Chunk {
    zone: Arc<Zone>,
    ptr: NonNull<u8>,
    len: usize,
}

unsafe impl Send for Chunk {}
unsafe impl Sync for Chunk {}

impl Chunk {
    /// Return the `Zone` this chunk belongs to.
    pub fn zone(&self) -> &Arc<Zone> {
        &self.zone
    }

    /// Get the total number of entities currently stored in this chunk.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get the total capacity of this chunk (usually this is fixed).
    pub fn capacity(&self) -> usize {
        self.zone.capacity
    }

    /// Get the memory layout of this chunk.
    pub fn layout(&self) -> Layout {
        self.zone.chunk_layout()
    }

    /// Get the offset into the chunk storage for a given component list.
    pub fn get_component_offset(&self, component_type: ComponentTypeID) -> Option<usize> {
        let iter = self.zone.component_offset_iter();

        for (ty, offset) in self.zone.archetype.component_types().iter().zip(iter) {
            if ty.type_id() == component_type {
                return Some(offset)
            }
        }

        None
    }

    /// Get a slice of components from this chunk.
    pub fn get_components<T: Component>(&self) -> Option<&[T]> {
        self.get_component_offset(T::type_id()).map(|offset| {
            unsafe {
                let ptr = self.ptr.as_ptr().offset(offset as isize) as *const T;
                &*std::ptr::slice_from_raw_parts(ptr, self.len)
            }
        })
    }

    /// Get a mutable list of components from this chunk.
    pub fn get_components_mut<T: Component>(&mut self) -> Option<&mut [T]> {
        self.get_component_offset(T::type_id()).map(|offset| {
            unsafe {
                let ptr = self.ptr.as_ptr().offset(offset as isize) as *mut T;
                &mut *std::ptr::slice_from_raw_parts_mut(ptr, self.len)
            }
        })
    }

    /// Create a new `ChunkWriter` for accessing multiple component lists at the
    /// same time.
    pub fn writer(&mut self) -> ChunkWriter {
        ChunkWriter::new(self)
    }

    /// Replace an entity in this chunk with the contents of a `ComponentSource`.
    pub fn replace_at(&mut self, index: usize, component_source: &impl ComponentSource) {
        assert!(self.len > index, "Items can only be replaced at positions up to len()");
        let components = self.zone.archetype.component_types().iter()
            .zip(self.zone.component_offset_iter());

        for (ty, offset) in components {
            let layout = ty.layout();
            let size = layout.size();

            let item_offset = offset + (size * index);

            unsafe {
                let slice = {
                    let ptr = self.ptr.as_ptr().offset(item_offset as isize);
                    &mut *std::ptr::slice_from_raw_parts_mut(ptr, size)
                };

                component_source.set_component(ty, slice);
            }
        }
    }

    /// Insert an entity into this chunk, using the factory function provided to
    /// fill components.
    pub fn insert(&mut self, index: usize, component_source: &impl ComponentSource) {
        assert!(self.len >= index, "Items can only be inserted at positions up to len()");

        let num_to_move = self.len - index;
        self.len += 1;
        let components = self.zone.archetype.component_types().iter()
            .zip(self.zone.component_offset_iter());

        for (ty, offset) in components {
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

                let slice = {
                    let ptr = self.ptr.as_ptr().offset(begin_offset as isize);
                    &mut *std::ptr::slice_from_raw_parts_mut(ptr, size)
                };

                component_source.set_component(ty, slice);
            }
        }
    }

    /// Insert a number of entities from another chunk at the given index.
    pub fn insert_from(&mut self, insert_at: usize, other: &Chunk, source_index: usize, n: usize) {
        assert!(std::ptr::eq(&*self.zone, &*other.zone), "chunks can only share elements in the same zone");
        assert!(self.len + n <= self.capacity());

        let num_to_move = self.len - insert_at;
        self.len += n;
        let components = self.zone.archetype.component_types().iter()
            .zip(self.zone.component_offset_iter());

        for (ty, offset) in components {
            let layout = ty.layout();
            let size = layout.size();
            let insert_offset = offset + (size * insert_at);
            let source_offset = offset + (size * source_index);

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
    pub fn remove(&mut self, index: usize, n: usize) {
        assert!(self.len > index, "Removal index must be < len()");
        assert!(self.len >= n);

        let num_to_move = self.len - index - n;
        self.len -= n;
        let components = self.zone.archetype.component_types().iter()
            .zip(self.zone.component_offset_iter());

        if num_to_move > 0 {
            for (ty, offset) in components {
                let layout = ty.layout();
                let size = layout.size();
                let begin_offset = offset + (size * index);
                let count = size * num_to_move;

                unsafe {
                    let to = self.ptr.as_ptr().offset(begin_offset as isize);
                    let from = to.offset((n * size) as isize);
                    ptr::copy(from, to, count);
                }
            }
        }
    }
}

impl Clone for Chunk {
    fn clone(&self) -> Self {
        let zone = self.zone.clone();
        let ptr = self.zone.allocate_page();
        let layout = self.layout();

        if self.len > 0 {
            unsafe { std::ptr::copy(self.ptr.as_ptr(), ptr.as_ptr(), layout.size()) };
        }

        Chunk {
            zone,
            ptr,
            len: self.len,
        }
    }
}

impl Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,
            "Chunk {{ zone: {:?}, len: {} }}",
            self.zone.as_ref() as *const _,
            self.len)
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        unsafe { self.zone.free_page(self.ptr) };
    }
}

/// A utility for accessing multiple different component types from a 
/// chunk at the same time.
/// 
/// This is designed to be used when you need to read and write to multiple
/// component types in the same chunk at the same time.
/// 
/// At the moment, it is impossible to get two references to the same component
/// slice from `ChunkWriter` mutable or not.
pub struct ChunkWriter<'a> {
    chunk: &'a mut Chunk,
    locked: BitVec,
}

impl<'a> ChunkWriter<'a> {
    /// Construct a new `ChunkWriter` from a mutable `Chunk` reference.
    pub fn new(chunk: &mut Chunk) -> ChunkWriter {
        let num_components = chunk.zone().archetype().component_types().len();

        ChunkWriter{
            chunk,
            locked: BitVec::from_elem(num_components, false),
        }
    }

    /// Get the slice of all components of the given type in the chunk.
    pub fn get_components<T: Component>(&mut self) -> Option<&'a [T]> {
        let types = self.chunk.zone().archetype().component_types();
        let type_index = types.binary_search_by_key(&T::type_id(), |ct| ct.type_id()).ok();
        
        if type_index.and_then(|idx| self.locked.get(idx)) == Some(false) {
            self.locked.set(type_index.unwrap(), true);
            let types = unsafe { std::mem::transmute(self.chunk.get_components::<T>().unwrap()) };
            Some(types)
        } else {
            None
        }
    }

    /// Get the mutable slice of all components of the given type in the chunk.
    pub fn get_components_mut<T: Component>(&mut self) -> Option<&'a mut [T]> {
        let types = self.chunk.zone().archetype().component_types();
        let type_index = types.binary_search_by_key(&T::type_id(), |ct| ct.type_id()).ok();
        
        if type_index.and_then(|idx| self.locked.get(idx)) == Some(false) {
            self.locked.set(type_index.unwrap(), true);
            let types = unsafe { std::mem::transmute(self.chunk.get_components_mut::<T>().unwrap()) };
            Some(types)
        } else {
            None
        }
    }
}

/// An ordered set of chunks.
/// 
/// `ChunkSet`s are used to manage a dynamic lists of chunks such that any
/// number of entities of the same Archetype can be stored.
#[derive(Clone)]
pub struct ChunkSet {
    zone: Arc<Zone>,
    chunks: Vec<Arc<Chunk>>,
}

impl ChunkSet {
    /// Create a new empty `ChunkSet` for the given `Zone`.
    pub fn new(zone: Arc<Zone>) -> ChunkSet {
        ChunkSet {
            zone,
            chunks: Vec::new(),
        }
    }

    /// Get the `Zone` the `Chunk`s in this `ChunkSet` are allocated from.
    pub fn zone(&self) -> &Arc<Zone> {
        &self.zone
    }

    /// Get the slice of all chunks in this `ChunkSet`.
    pub fn chunks(&self) -> &[Arc<Chunk>] {
        &self.chunks
    }

    /// Get a mutable reference to a single chunk in the `ChunkSet`.
    pub fn chunk_mut(&mut self, index: usize) -> Option<&mut Arc<Chunk>> {
        self.chunks.get_mut(index)
    }

    /// Search for a given entity in the `ChunkSet`, an `Ok()` returns the index
    /// of the entity in the `ChunkSet`, an `Err()` returns the index where it
    /// would be inserted.
    pub fn binary_search(&self, entity_id: EntityID) -> Result<(usize, usize), (usize, usize)> {
        let mut lo = 0;
        let mut hi = self.chunks.len();

        while lo < hi {
            let mid = (lo + hi) / 2;
            let chunk = &self.chunks[mid];
            let ids = chunk.get_components::<EntityID>().unwrap();

            if mid > 0 {
                if let Some(bound) = ids.get(0) {
                    if entity_id.cmp(&bound) == Ordering::Less {
                        hi = mid;
                        continue;
                    }
                }
            }

            if let Some(chunk) = self.chunks.get(mid + 1) {
                let ids = chunk.get_components::<EntityID>().unwrap();
                if let Some(bound) = ids.get(0) {
                    if entity_id.cmp(&bound) != Ordering::Less {
                        lo = mid + 1;
                        continue;
                    }
                }
            }

            // We're in the right chunk!
            return match ids.binary_search(&entity_id) {
                Ok(idx) => Ok((mid, idx)),
                Err(idx) => Err((mid, idx)),
            };
        }

        Err((0, 0))
    }
    
    /// Push a new entity to the back of this `ChunkSet`.
    pub fn push(&mut self, components: &impl ComponentSource) -> (usize, usize) {
        let (chunk_index, index) = if self.chunks.len() == 0 {
            (0, 0)
        } else {
            let chunk_index = self.chunks.len() - 1;
            let index = self.chunks[chunk_index].len();
            (chunk_index, index)
        };

        self.insert_at(chunk_index, index, components)
    }

    /// Insert an entity at the given position, returning the next position after
    /// the inserted entity.
    pub fn insert_at(&mut self, chunk_index: usize, index: usize, components: &impl ComponentSource) -> (usize, usize) {
        // Special case: inserting the first chunk.
        if self.chunks.len() == 0 {
            assert_eq!(chunk_index, 0);
            assert_eq!(index, 0);

            let mut chunk = self.zone.clone().new_chunk();
            chunk.insert(0, components);
            self.chunks.push(Arc::new(chunk));
            return (0, 1);
        }

        let chunk = Arc::make_mut(&mut self.chunks[chunk_index]);
        let cap = chunk.capacity();
        let half_cap = cap / 2;

        if chunk.len() < cap {
            // Just insert the entity!
            chunk.insert(index, components);
            (chunk_index, index + 1)
        } else {
            // Not enough room, split the chunk!
            let mut right_chunk = chunk.zone().clone().new_chunk();
            right_chunk.insert_from(0, chunk, half_cap, half_cap);
            chunk.remove(half_cap, half_cap);

            let new_pos = if index > half_cap {
                right_chunk.insert(index - half_cap, components);
                (chunk_index + 1, index - half_cap + 1)
            } else {
                chunk.insert(index, components);
                (chunk_index, index + 1)
            };

            self.chunks.insert(chunk_index + 1, Arc::new(right_chunk));
            new_pos
        }
    }

    /// Remove an entity from this `ChunkSet`, returning the position after
    /// the entity in the new layout.
    pub fn remove_at(&mut self, chunk_index: usize, index: usize) -> (usize, usize) {
        let max_chunk_size = self.zone.chunk_capacity();
        let chunk = &self.chunks[chunk_index];

        // Special case: removing the final entity!
        if chunk.len() == 1 && self.chunks.len() == 1 {
            assert_eq!(chunk_index, 0);
            assert_eq!(index, 0);
            self.chunks.clear();
            return (0, 0);
        }

        let needs_merge = (chunk.len() <= max_chunk_size / 2) && self.chunks.len() > 1;

        if needs_merge {
            let left_len = if chunk_index > 0 {
                Some(self.chunks[chunk_index - 1].len())
            } else {
                None
            };
            let right_len = self.chunks.get(chunk_index + 1).map(|x| x.len());
            let merge_left = right_len.is_none() || (left_len.unwrap() <= right_len.unwrap());
            
            if merge_left {
                let (left_chunk, right_chunk) = {
                    let pair = &mut self.chunks[chunk_index-1..chunk_index+1];
                    let (left, rest) = pair.split_first_mut().unwrap();
                    (Arc::make_mut(left), Arc::make_mut(&mut rest[0]))
                };

                let left_len = left_len.unwrap();
                let right_len = right_chunk.len();
                let total_len = right_len + left_len - 1;

                if total_len <= max_chunk_size {
                    // Genuine merge
                    left_chunk.insert_from(left_chunk.len(), right_chunk, 0, index);
                    let new_index = left_chunk.len();
                    let after_split = right_chunk.len() - index - 1;
                    left_chunk.insert_from(left_chunk.len(), right_chunk, index + 1, after_split);

                    self.chunks.remove(chunk_index);
                    (chunk_index - 1, new_index)
                } else {
                    // Rebalance
                    let split_at = total_len / 2;
                    right_chunk.remove(index, 1);

                    if left_chunk.len() > right_chunk.len() {
                        // Move from left chunk to right chunk.
                        let n = left_chunk.len() - split_at;
                        let source_idx = left_chunk.len() - n;
                        right_chunk.insert_from(0, left_chunk, source_idx, n);
                        left_chunk.remove(source_idx, n);

                    } else {
                        // Move from right chunk to left.
                        let n = right_chunk.len() - split_at;
                        left_chunk.insert_from(left_chunk.len(), right_chunk, 0, n);
                        right_chunk.remove(0, n);
                    }

                    let from_right = right_len - index;
                    if from_right <= split_at {
                        (chunk_index, right_chunk.len() - from_right)
                    } else {
                        (chunk_index - 1, left_chunk.len() + right_chunk.len() - from_right)
                    }
                }
            } else {
                let (left_chunk, right_chunk) = {
                    let pair = &mut self.chunks[chunk_index..chunk_index+2];
                    let (left, rest) = pair.split_first_mut().unwrap();
                    (Arc::make_mut(left), Arc::make_mut(&mut rest[0]))
                };

                let left_len = left_chunk.len();
                let right_len = right_len.unwrap();
                let total_len = right_len + left_len - 1;

                left_chunk.remove(index, 1);

                if total_len <= max_chunk_size {
                    // Genuine merge
                    left_chunk.insert_from(left_chunk.len(), right_chunk, 0, right_chunk.len());

                    self.chunks.remove(chunk_index + 1);
                    (chunk_index, index)
                } else {
                    // Rebalance
                    let split_at = total_len / 2;

                    if left_chunk.len() > right_chunk.len() {
                        // Move from left chunk to right chunk.
                        let n = left_chunk.len() - split_at;
                        let source_idx = left_chunk.len() - n;
                        right_chunk.insert_from(0, left_chunk, source_idx, n);
                        left_chunk.remove(source_idx, n);

                    } else {
                        // Move from right chunk to left.
                        let n = right_chunk.len() - split_at;
                        left_chunk.insert_from(left_chunk.len(), right_chunk, 0, n);
                        right_chunk.remove(0, n);
                    }

                    if index >= split_at {
                        (chunk_index + 1, index - left_chunk.len())
                    } else {
                        (chunk_index, index)
                    }
                }
            }
        } else {
            let mut_chunk = Arc::make_mut(&mut self.chunks[chunk_index]);
            mut_chunk.remove(index, 1);
            (chunk_index, index)
        }
    }
}
