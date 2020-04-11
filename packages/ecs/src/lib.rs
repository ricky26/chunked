#![allow(dead_code, unused_macros)]

use std::fmt::Display;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::fmt::{self, Debug};
use std::alloc::{self, Layout};
use std::sync::{self, Arc, atomic::{self, AtomicUsize}};
use std::ptr::{self, NonNull};
use std::cmp::{Ord, Ordering};
use std::future::Future;
use std::task::Poll;
use once_cell::sync::OnceCell;
use futures::lock::Mutex;
use futures::future;
use arc_swap::{ArcSwap};
use sorted_vec::{VecSet, VecMap};
use bit_vec::BitVec;
use crossbeam_queue::{ArrayQueue, PushError};
use async_trait::async_trait;
use tokio::task::JoinHandle;

mod sorted_vec;

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

#[derive(Debug,Clone,Copy,PartialEq,Eq,PartialOrd,Ord)]
pub struct ComponentTypeID(usize);
static mut NEXT_TYPE_ID: AtomicUsize = AtomicUsize::new(1);

impl ComponentTypeID {
    pub unsafe fn new(inner: usize) -> ComponentTypeID {
        ComponentTypeID(inner)
    }

    pub fn unique() -> ComponentTypeID {
        let id = unsafe { NEXT_TYPE_ID.fetch_add(1, atomic::Ordering::Relaxed) };
        ComponentTypeID(id)
    }

    pub fn id(&self) -> usize {
        self.0
    }
}

pub struct AutoComponentTypeID(OnceCell<ComponentTypeID>);

impl AutoComponentTypeID {
    pub const fn new() -> AutoComponentTypeID {
        AutoComponentTypeID(OnceCell::new())
    }

    pub fn get(&self) -> ComponentTypeID {
        self.0.get_or_init(|| ComponentTypeID::unique()).clone()
    }
}

pub trait Component: Default + Copy {
    fn type_id() -> ComponentTypeID;
    fn layout() -> Layout;
}

#[macro_export]
macro_rules! component {
    ($i:ident) => {
        const _: () = {
            static INIT_TYPE: ::ecs::AutoComponentTypeID = ::ecs::AutoComponentTypeID::new();

            impl ::ecs::Component for $i {
                fn type_id() -> ::ecs::ComponentTypeID {
                    INIT_TYPE.get()
                }

                fn layout() -> ::core::alloc::Layout {
                    ::core::alloc::Layout::new::<$i>()
                }
            }

            ()
        };
    };
}

#[derive(Clone,Copy)]
pub struct ComponentType {
    type_id: ComponentTypeID,
    layout: Layout,
    set_default: fn(&mut [u8]),
}

impl ComponentType {
    pub fn for_type<T: Component>() -> ComponentType {
        fn default<T: Component>(ptr: &mut [u8]) {
            assert!(ptr.len() == std::mem::size_of::<T>());
            let ptr = unsafe { std::mem::transmute(ptr.as_ptr()) };
            let d = T::default();
            std::mem::replace(ptr, d);
        }

        ComponentType{
            type_id: T::type_id(),
            layout: T::layout(),
            set_default: default::<T>,
        }
    }

    pub fn type_id(&self) -> ComponentTypeID {
        self.type_id
    }

    pub fn layout(&self) -> Layout {
        self.layout
    }

    pub fn set_default(&self, ptr: &mut [u8]) {
        (self.set_default)(ptr)
    }
}

impl PartialEq for ComponentType {
    fn eq(&self, other: &ComponentType) -> bool {
        self.type_id.eq(&other.type_id)
    }
}

impl Eq for ComponentType {}

impl PartialOrd for ComponentType {
    fn partial_cmp(&self, other: &ComponentType) -> Option<Ordering> {
        self.type_id.partial_cmp(&other.type_id)
    }
}

impl Ord for ComponentType {
    fn cmp(&self, other: &ComponentType) -> Ordering {
        self.type_id.cmp(&other.type_id)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Archetype {
    component_types: Vec<ComponentType>,
}

impl Archetype {
    pub fn new(component_types: Vec<ComponentType>) -> Archetype {
        Archetype{
            component_types: component_types,
        }
    }

    /// Return the sorted list of component types in this archetype.
    pub fn component_types(&self) -> &[ComponentType] {
        &self.component_types
    }
}

/// An iterator over components of a single type contained in an archetype.
pub struct EntityComponentIterator {
    archetype: Option<Arc<Archetype>>,
    component_index: usize,
}

impl EntityComponentIterator {
    /// Create a new empty `EntityComponentIterator`.
    pub fn empty() -> EntityComponentIterator {
        EntityComponentIterator{
            archetype: None,
            component_index: 0,
        }
    }

    /// Create an `EntityComponentIterator` for a given type.
    pub fn for_archetype(archetype: Arc<Archetype>) -> EntityComponentIterator {
        EntityComponentIterator{
            archetype: Some(archetype),
            component_index: 0,
        }
    }
}

impl Iterator for EntityComponentIterator {
    type Item = ComponentTypeID;

    fn next(&mut self) -> Option<ComponentTypeID> {
        if let Some(archetype) = &self.archetype {
            let types = &archetype.component_types;
            if self.component_index >= types.len() {
                None
            } else {
                let type_id = types[self.component_index].type_id;
                self.component_index += 1;
                Some(type_id)
            }
        } else {
            None
        }
    }
}

#[derive(Debug,Clone,Copy,Default,PartialEq,Eq,PartialOrd,Ord,Hash)]
pub struct EntityID(usize);

impl Component for EntityID {
    fn type_id() -> ComponentTypeID {
        unsafe { ComponentTypeID::new(0) }
    }

    fn layout() -> Layout {
        Layout::new::<Self>()
    }
}

impl EntityID {
    pub fn new(id: usize) -> EntityID {
        EntityID(id)
    }

    pub fn id(&self) -> usize {
        self.0
    }
}

pub struct ComponentOffsetIterator {
    archetype: Arc<Archetype>,
    capacity: usize,
    type_index: usize,
    offset: usize,
    align: usize,
}

impl ComponentOffsetIterator {
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
        Layout::from_size_align(self.len(), self.align()).unwrap()
    }

    pub fn len(&self) -> usize {
        self.offset.max(1)
    }

    pub fn align(&self) -> usize {
        self.align
    }
}

impl Iterator for ComponentOffsetIterator {
    type Item = usize;
 
    fn next(&mut self) -> Option<usize> {
        if self.type_index >= self.archetype.component_types.len() {
            None
        } else {
            let ty = self.archetype.component_types[self.type_index];
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

/// The trait used to convey raw component data.
pub unsafe trait ComponentSource {
    fn set_component(&self, component_type: &ComponentType, storage: &mut [u8]);
}

pub struct DefaultComponentSource;

unsafe impl ComponentSource for DefaultComponentSource {
    fn set_component(&self, component_type: &ComponentType, storage: &mut [u8]) {
        component_type.set_default(storage);
    }
}

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
        let chunk_layout = {
            let mut iter = ComponentOffsetIterator::new(archetype.clone(), capacity);
            while let Some(_) = iter.next() {}
            Layout::from_size_align(iter.len(), iter.align()).unwrap()
        };

        Zone {
            archetype,
            capacity,
            key,
            chunk_layout,
            free_list: ArrayQueue::new(256),
        }
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

    /// Get a raw pointer to the underlying archetype, for comparison.
    fn archetype_ptr(&self) -> *const Archetype {
        self.archetype.as_ref() as *const _
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

        for (ty, offset) in self.zone.archetype.component_types.iter().zip(iter) {
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
        let components = self.zone.archetype.component_types.iter()
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
        let components = self.zone.archetype.component_types.iter()
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
        let components = self.zone.archetype.component_types.iter()
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
        let components = self.zone.archetype.component_types.iter()
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

pub struct ChunkWriter<'a> {
    chunk: &'a mut Chunk,
    locked: BitVec,
}

impl<'a> ChunkWriter<'a> {
    pub fn new(chunk: &mut Chunk) -> ChunkWriter {
        let num_components = chunk.zone().archetype().component_types().len();

        ChunkWriter{
            chunk,
            locked: BitVec::from_elem(num_components, false),
        }
    }

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

#[derive(Clone)]
pub struct ChunkSet {
    zone: Arc<Zone>,
    chunks: Vec<Arc<Chunk>>,
}

impl ChunkSet {
    pub fn new(zone: Arc<Zone>) -> ChunkSet {
        ChunkSet {
            zone,
            chunks: Vec::new(),
        }
    }

    pub fn zone(&self) -> &Arc<Zone> {
        &self.zone
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
    fn push(&mut self, components: &impl ComponentSource) -> (usize, usize) {
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
    fn insert_at(&mut self, chunk_index: usize, index: usize, components: &impl ComponentSource) -> (usize, usize) {
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
    fn remove_at(&mut self, chunk_index: usize, index: usize) -> (usize, usize) {
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

pub struct SnapshotWriter {
    num_chunks: usize,
    contents: sync::Mutex<(Arc<Snapshot>, BitVec)>,
    cond: sync::Condvar,
}

impl SnapshotWriter {
    pub fn new(snapshot: Arc<Snapshot>) -> SnapshotWriter {
        let num_chunks = snapshot.chunk_sets.iter()
            .map(|cs| cs.chunks.len())
            .sum();

        SnapshotWriter {
            num_chunks,
            contents: sync::Mutex::new((snapshot, BitVec::from_elem(num_chunks, false))),
            cond: sync::Condvar::new(),
        }
    }

    pub fn into_inner(self) -> Arc<Snapshot> {
        self.contents.into_inner().unwrap().0
    }

    pub fn num_chunks(&self) -> usize {
        self.num_chunks
    }

    fn set_snapshot(&self, snapshot: Arc<Snapshot>) {
        let mut contents = self.contents.lock().unwrap();
        contents.0 = snapshot;
    }

    pub fn borrow_chunk_mut(&self, index: usize) -> SnapshotWriterGuard {
        let (chunk_set, chunk_index) = loop {
            let mut contents = self.contents.lock().unwrap();

            let (chunk_set_index, chunk_index) = {
                let mut chunk_index = index;
                let mut chunk_set_index = 0;

                for chunk_set in contents.0.chunk_sets.iter() {
                    if chunk_set.chunks.len() <= chunk_index {
                        chunk_set_index += 1;
                        chunk_index -= chunk_set.chunks.len();
                    } else {
                        break;
                    }
                }

                (chunk_set_index, chunk_index)
            };

            let index = contents.0.chunk_sets.iter()
                .map(|cs| cs.chunks.len())
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

        let chunk = Arc::make_mut(&mut chunk_set.chunks[chunk_index]);
        SnapshotWriterGuard {
            writer: self,
            chunk,
            index,
        }
    }

    fn free_chunk(&self, index: usize) {
        let mut contents = self.contents.lock().unwrap();
        contents.1.set(index, false);
        self.cond.notify_all();
    }
}

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

pub struct World {
    archetypes: ArcSwap<VecSet<Arc<Archetype>>>,
    zones: ArcSwap<Vec<Arc<Zone>>>,
    current_snapshot: ArcSwap<Snapshot>,

    update_lock: Mutex<()>,
    next_entity_id: AtomicUsize,
    next_zone_id: AtomicUsize,
    max_chunk_size: usize,
}

impl World {
    /// Create a new world with the default settings.
    pub fn new() -> World {
        World::with_max_chunk_size(4096)
    }

    /// Create a new world with a specified maximum number of entities per chunk.
    pub fn with_max_chunk_size(max_chunk_size: usize) -> World {
        World {
            archetypes: ArcSwap::from_pointee(VecSet::new()),
            zones: ArcSwap::from_pointee(Vec::new()),
            current_snapshot: ArcSwap::from_pointee(Snapshot::empty()),

            update_lock: Mutex::new(()),
            next_zone_id: AtomicUsize::new(1),
            next_entity_id: AtomicUsize::new(1),
            max_chunk_size: max_chunk_size,
        }
    }

    /// Get a zone with the given component types, providing it already exists
    /// in the world.
    /// 
    /// Generally, one should prefer `get_archetype` unless you have a
    /// particular reason to not want the archetype to be created, as it handles
    /// the fast case of the archetype already existing.
    pub fn get_archetype(&self, component_types: &mut Vec<ComponentType>) -> Option<Arc<Archetype>> {
        let entity_component_type = ComponentType::for_type::<EntityID>();

        component_types.dedup();
        if !component_types.contains(&entity_component_type) {
            component_types.push(entity_component_type);
        }
        component_types.sort();

        let archetypes = self.archetypes.load();
        archetypes.binary_search_by_key(&&component_types[..], |archetype| &*archetype.component_types)
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
        let archetypes = &self.archetypes;
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
            let zones = self.zones.load();

            return match zones.binary_search_by_key(&(&*archetype as &Archetype as *const Archetype), |z| &*z.archetype() as &Archetype as *const Archetype) {
                Ok(index) => {
                    zones[index].clone()
                },
                Err(index) => {
                    if new_zone.is_none() {
                        let zone_id = self.next_zone_id.fetch_add(1, atomic::Ordering::Relaxed);
                        new_zone = Some(Arc::new(Zone::new(archetype.clone(), self.max_chunk_size, zone_id)));
                    }

                    let mut new_zones = Vec::with_capacity(zones.len() + 1);
                    new_zones.extend(zones[..index].iter().cloned());
                    new_zones.push(new_zone.clone().unwrap());
                    new_zones.extend(zones[index..].iter().cloned());

                    let next_value = self.zones.compare_and_swap(&zones, Arc::new(new_zones));
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
        EntityID::new(self.next_entity_id.fetch_add(1, atomic::Ordering::Relaxed))
    }

    /// Create a snapshot of the current world state.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.current_snapshot.load_full()
    }
}

impl Debug for World {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "World {{\n")?;
        write!(f, "  Zones:\n")?;

        for zone in self.zones.load().iter() {
            let archetype = &zone.archetype;

            write!(f, "    #{} - ", zone.key)?;
            for ty in archetype.component_types.iter() {
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
    chunk_sets: Vec<ChunkSet>,
}

impl Snapshot {
    /// Create a new snapshot of an empty world.
    pub fn empty() -> Snapshot {
        Snapshot {
            chunk_sets: Vec::new(),
        }
    }

    /// Create an iterator over all the chunks in the snapshot.
    pub fn iter_chunks(&self) -> impl Iterator<Item=&Arc<Chunk>> {
        self.chunk_sets.iter().flat_map(|cs| &cs.chunks)
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
            for chunk in chunk_set.chunks.iter() {
                write!(f, "    {:?} - zone {} - {} entities\n", chunk.deref() as *const _, chunk.zone().key, chunk.len())?;
            }
        }

        write!(f, "  Entities:\n")?;        
        for chunk_set in self.chunk_sets.iter() {
            for chunk in chunk_set.chunks.iter() {
                let ids = chunk.get_components::<EntityID>().unwrap();

                for entity_id in ids {
                    write!(f, "    Entity {:?} - Chunk {:?}\n", entity_id, chunk.deref() as *const _)?;
                }
            }
        }

        write!(f, "}}\n")
    }
}

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
        let chunk = &chunk_set.chunks[self.chunk_index];
        
        chunk.get_components::<T>()
            .and_then(|slice| slice.get(self.entity_index))
    }
}

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
        let archetypes = world.archetypes.load();

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

            while chunk_index <= chunk_set.chunks.len() {
                let chunk = chunk_set.chunks.get(chunk_index);
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
                    let chunk_mut = Arc::make_mut(&mut chunk_set_mut.chunks[chunk_index]);
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

                if snapshot.chunk_sets.binary_search_by_key(&zone.key, |cs| cs.zone().key).is_ok() {
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

                match new_chunk_sets.binary_search_by_key(&zone.key, |cs| cs.zone().key) {
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
                        old_v.zone().key > new_v.zone().key
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
        let _lock = world.update_lock.lock().await;
        world.current_snapshot.store(self.mutate_snapshot(world.snapshot()));
    }
}

/*
pub struct ComponentSet<'a> {
    read_chunk: Arc<Chunk>,
    write_chunk: Arc<Chunk>,

    read: &'a [ComponentType],
    write: &'a [ComponentType],

    in_use_mask: usize,
}

impl<'a> ComponentSet<'a> {
    pub fn read<T: Component>(&mut self) -> Option<&'a [T]> {
        if let Ok(idx) = self.read.binary_search_by_key(&T::type_id(), |ct| ct.type_id()) {
            let bit = 1 << idx;
            assert!(bit != 0, "component exceeds max componentcount");

            if (self.in_use_mask & bit) != 0 {
                self.in_use_mask |= bit;
                unsafe { std::mem::transmute(self.read_chunk.get_components::<T>()) }
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn write<T: Component>(&mut self) -> Option<&'a mut [T]> {
        if let Ok(idx) = self.read.binary_search_by_key(&T::type_id(), |ct| ct.type_id()) {
            let bit = 1 << (idx + self.read.len());
            assert!(bit != 0, "component exceeds max componentcount");

            if (self.in_use_mask & bit) != 0 {
                self.in_use_mask |= bit;

                let chunk_mut = Arc::make_mut(&mut self.write_chunk);
                unsafe { std::mem::transmute(chunk_mut.get_components_mut::<T>()) }
            } else {
                None
            }
        } else {
            None
        }
    }
}

pub struct ComponentSets<'a> {
    read_snapshot: Arc<Snapshot>,
    write_snapshot: Arc<Snapshot>,

    read: &'a [ComponentType],
    write: &'a [ComponentType],

    has_written: atomic::AtomicBool,
}

impl ComponentSets {
    pub fn read(&self, f: impl for<'b> FnMut(ComponentSet<'b>)) {


    }

    pub fn write(&self, f: impl for<'b> FnMut(ComponentSet<'b>)) {

    }
}
*/

#[async_trait]
pub trait ChunkSystem {
    async fn update(&mut self, snapshot: &Arc<Snapshot>, writer: &SnapshotWriter);
}

#[async_trait]
pub trait SnapshotSystem {
    async fn update(&mut self, snapshot: &Arc<Snapshot>) -> Arc<Snapshot>;
}

enum SystemRef {
    Snapshot(Box<dyn SnapshotSystem + Send + 'static>),
    Chunk(Box<dyn ChunkSystem + Send + 'static>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SystemToken(pub usize);

pub struct SystemRegistration {
    system: SystemRef,
    before: Vec<SystemToken>,
    after: Vec<SystemToken>,
    read: Vec<ComponentType>,
    write: Vec<ComponentType>,
}

impl SystemRegistration {
    fn new(system: SystemRef) -> SystemRegistration {
        SystemRegistration {
            system,
            before: Vec::new(),
            after: Vec::new(),
            read: Vec::new(),
            write: Vec::new(),
        }
    }

    pub fn new_snapshot(system: impl SnapshotSystem + Send + 'static) -> SystemRegistration {
        let boxed = Box::new(system) as Box<dyn SnapshotSystem + Send + 'static>;
        SystemRegistration::new(SystemRef::Snapshot(boxed))
    }

    pub fn new_chunk(system: impl ChunkSystem + Send + 'static) -> SystemRegistration {
        let boxed = Box::new(system) as Box<dyn ChunkSystem + Send + 'static>;
        SystemRegistration::new(SystemRef::Chunk(boxed))
    }

    pub fn before(mut self, system: SystemToken) -> Self {
        if let Err(insert_idx) = self.before.binary_search(&system) {
            self.before.insert(insert_idx, system);
        }

        self
    }

    pub fn after(mut self, system: SystemToken) -> Self {
        if let Err(insert_idx) = self.after.binary_search(&system) {
            self.after.insert(insert_idx, system);
        }

        self
    }

    pub fn read_component_type(mut self, component_type: ComponentType) -> Self {
        self.read.push(component_type);
        self
    }

    pub fn read<T: Component>(self) -> Self {
        self.read_component_type(ComponentType::for_type::<T>())
    }

    pub fn write_component_type(mut self, component_type: ComponentType) -> Self {
        self.write.push(component_type);
        self
    }

    pub fn write<T: Component>(self) -> Self {
        self.write_component_type(ComponentType::for_type::<T>())
    }
}

#[derive(Clone, Debug)]
pub struct SystemRegistrationError;

impl Display for SystemRegistrationError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "conflicting system requirements")
    }
}

impl std::error::Error for SystemRegistrationError {
}

struct SystemSetSystem {
    registration: SystemRegistration,
    token: SystemToken,
    phase: usize,
}

impl SystemSetSystem {
    pub fn new(registration: SystemRegistration, token: SystemToken) -> SystemSetSystem {
        SystemSetSystem {
            registration,
            token,
            phase: 0,
        }
    }

    pub fn will_write(&self) -> bool {
        match &self.registration.system {
            &SystemRef::Snapshot(_) => true,
            &SystemRef::Chunk(_) => !self.registration.write.is_empty(),
        }
    }

    /// Spawn an execution of this system.
    /// 
    /// This is unsafe as you /must/ wait for the task to complete before using
    /// this system for anything else.
    pub unsafe fn spawn(&mut self, snapshot: &Arc<Snapshot>, writer: &SnapshotWriter) -> JoinHandle<()> {
        let mut sys: &mut SystemRef = std::mem::transmute(&mut self.registration.system);
        let snapshot = snapshot.clone();
        let writer: &'static SnapshotWriter = std::mem::transmute(writer);

        tokio::task::spawn(async move {
            match &mut sys {
                &mut SystemRef::Chunk(sys) => sys.update(&snapshot, writer).await,
                &mut SystemRef::Snapshot(sys) => {
                    let snapshot = sys.update(&snapshot).await;
                    writer.set_snapshot(snapshot);
                },
            }
        })
    }
}

/// A set of systems which are used to modify a world.
pub struct SystemSet {
    next_system_id: usize,
    num_phases: usize,
    systems: Vec<SystemSetSystem>,
    systems_dirty: bool,

    active_systems: Vec<(JoinHandle<()>, bool)>,
}

impl SystemSet {
    pub fn new() -> SystemSet {
        SystemSet {
            next_system_id: 1,
            num_phases: 0,
            systems: Vec::new(),
            systems_dirty: false,
            active_systems: Vec::new(),
        }
    }

    pub fn insert(&mut self, system: SystemRegistration) -> Result<SystemToken, SystemRegistrationError> {
        let token = SystemToken(self.next_system_id);
        self.next_system_id += 1;
        let system = SystemSetSystem::new(system, token);

        let lo = system.registration.after.iter()
            .filter_map(|token| self.systems.binary_search_by_key(token, |r| r.token).ok())
            .max()
            .unwrap_or(0);
        let hi = system.registration.before.iter()
            .filter_map(|token| self.systems.binary_search_by_key(token, |r| r.token).ok())
            .min()
            .unwrap_or(self.systems.len());

        if lo <= hi {
            self.systems.insert(hi, system);
            self.systems_dirty = true;
            Ok(token)
        } else {
            Err(SystemRegistrationError)
        }
    }

    fn update_systems(&mut self) {
        self.systems_dirty = false;

        let mut current_phase = 0;

        for sys in self.systems.iter_mut() {
            sys.phase = current_phase;
            current_phase += 1;
        }
        
        self.num_phases = current_phase + 1;
    }

    pub async fn update(&mut self, world: &Arc<World>) {
        if self.systems_dirty {
            self.update_systems()
        }

        let _lock = world.update_lock.lock().await;
        let mut num_woken = 0;
        let mut num_writers: usize = 0;
        let mut snapshot = world.snapshot();

        for phase in 0..self.num_phases {
            let writer = SnapshotWriter::new(snapshot.clone());

            while let Some(sys) = self.systems.get_mut(num_woken).filter(|s| s.phase == phase) {
                num_woken += 1;

                let will_write = sys.will_write();
                if will_write {
                    num_writers += 1;
                }

                self.active_systems.push((unsafe { sys.spawn(&snapshot, &writer) }, will_write));
            }

            while num_writers > 0 {
                let futures = &mut self.active_systems;
                let done_idx = future::poll_fn(|cx| {
                    for (idx, f) in futures.into_iter().enumerate() {
                        match Pin::new(&mut f.0).poll(cx) {
                            Poll::Ready(_) => return Poll::Ready(idx),
                            Poll::Pending => {},
                        }
                    }

                    Poll::Pending
                }).await;

                if futures[done_idx].1 {
                    num_writers -= 1;
                }

                self.active_systems.remove(done_idx);
            }

            snapshot = writer.into_inner();
        }

        while !self.active_systems.is_empty() {
            let futures = &mut self.active_systems;
            let done_idx = future::poll_fn(|cx| {
                for (idx, f) in futures.into_iter().enumerate() {
                    match Pin::new(&mut f.0).poll(cx) {
                        Poll::Ready(_) => return Poll::Ready(idx),
                        Poll::Pending => {},
                    }
                }

                Poll::Pending
            }).await;

            self.active_systems.remove(done_idx);
        }

        world.current_snapshot.store(snapshot);
    }
}
