use std::sync::{Arc, atomic};

use crate::{Chunk, Component, EntityID, Archetype};
use crate::world::Lock;
use crate::world::transaction::Transaction;

/// A guard that holds a read-write lock over a `Chunk`.
pub struct ChunkGuard<'a> {
    transaction: &'a Transaction,
    chunk: *const Chunk,
    taken: atomic::AtomicU32,
}

unsafe impl<'a> Send for ChunkGuard<'a> {}
unsafe impl<'a> Sync for ChunkGuard<'a> {}

impl<'a> ChunkGuard<'a> {
    /// Create a new chunk guard.
    pub(crate) fn new(transaction: &'a Transaction, chunk: &Arc<Chunk>) -> ChunkGuard<'a> {
        let chunk = &**chunk;
        ChunkGuard {
            transaction,
            chunk,
            taken: atomic::AtomicU32::new(0),
        }
    }

    fn get(&self) -> &'static mut Chunk {
        let ptr = self.chunk as *mut _;
        unsafe { &mut *ptr }
    }

    /// Get the archetype this chunk belongs to.
    pub fn archetype(&self) -> &Arc<Archetype> {
        self.get().archetype()
    }

    /// Get the number of entities in this chunk.
    pub fn len(&self) -> usize {
        self.get().len()
    }

    /// Returns true if this chunk contains no entities.
    pub fn is_empty(&self) -> bool {
        self.get().len() == 0
    }

    /// Get a set of components from this chunk.
    pub fn components<T: Component>(&self) -> Option<&[T]> {
        if T::type_id() == EntityID::type_id() {
            // EntityID is not required in locks.
            self.get().components::<T>()
        } else {
            let position = self.transaction.locks().iter()
                .position(|l| l == &Lock::Read(T::type_id()));

            position.and_then(|idx| {
                let taken = self.taken.fetch_or(1 << idx, atomic::Ordering::Relaxed);
                if (taken & (1 << idx)) == 0 {
                    self.get().components::<T>()
                } else {
                    None
                }
            })
        }
    }

    /// Get a mutable slice of a given type of components from a chunk.
    pub fn components_mut<T: Component>(&self) -> Option<&mut [T]> {
        let position = self.transaction.locks().iter()
            .position(|l| l == &Lock::Write(T::type_id()));

        position.and_then(|idx| {
            let taken = self.taken.fetch_or(1 << idx, atomic::Ordering::Relaxed);
            if (taken & (1 << idx)) == 0 {
                self.get().components_mut::<T>()
            } else {
                None
            }
        })
    }
}
