use std::sync::Arc;

use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use rayon::iter::plumbing::{bridge, Consumer, Producer, ProducerCallback, UnindexedConsumer};

use crate::{Archetype, Snapshot};
use crate::archetype::ComponentSetExt;
use crate::world::{ChunkGuard, ChunkSetGuard, Lock};

/// Checks whether a given lock set applies to an archetype.
pub(crate) fn locks_include_archetype(a: &Arc<Archetype>, locks: &[Lock]) -> bool {
    assert!(locks.len() <= 32, "only 32 locks supported currently");

    let component_types = a.component_types();
    for lock in locks.iter() {
        match lock {
            Lock::Read(type_id) => {
                if !component_types.includes(type_id) {
                    return false;
                }
            }
            Lock::Write(type_id) => {
                if !component_types.includes(type_id) {
                    return false;
                }
            }
            Lock::Exclude(type_id) => {
                if component_types.includes(type_id) {
                    return false;
                }
            }
        }
    }

    true
}

/// A single in-progress transaction.
pub(crate) struct Transaction {
    snapshot: Arc<Snapshot>,
    archetypes: Vec<Arc<Archetype>>,
    locks: Vec<Lock>,
}

impl Transaction {
    /// Create a new transaction.
    pub fn new(
        snapshot: Arc<Snapshot>,
        archetypes: Vec<Arc<Archetype>>,
        locks: Vec<Lock>,
    ) -> Transaction {
        Transaction {
            snapshot,
            archetypes,
            locks,
        }
    }

    /// Get the set of locks held by this transaction.
    pub fn locks(&self) -> &[Lock] {
        &self.locks
    }
}

/// A guard that has rw-locks over a subset of components in the snapshot.
///
/// Structural locks cannot be taken whilst a `TransactionGuard` exists.
pub struct TransactionGuard<'a> {
    transaction: &'a Transaction,
}

impl<'a> TransactionGuard<'a> {
    /// Create a new `TransactionGuard`.
    ///
    /// Only one of these should be made per `Transaction`.
    pub(crate) fn new(transaction: &'a Transaction) -> TransactionGuard<'a> {
        TransactionGuard {
            transaction,
        }
    }
}

impl<'a> TransactionGuard<'a> {
    /// Get all the involved archetypes in this transaction.
    pub fn archetypes(&self) -> &[Arc<Archetype>] {
        &self.transaction.archetypes
    }

    /// Fetch a single `ChunkSet` from this transaction.
    pub fn chunk_set_mut(&mut self, archetype: &Arc<Archetype>) -> Option<ChunkSetGuard<'_>> {
        match self.transaction.archetypes
            .binary_search_by_key(&archetype.id(), |a| a.id()) {
            Ok(_) => {
                let chunk_set = self.transaction.snapshot.chunk_set(archetype).unwrap();
                Some(ChunkSetGuard::new(self.transaction, archetype.clone(), chunk_set))
            }
            Err(_) => None
        }
    }

    /// Iterate over all `ChunkSet`s in this transaction.
    pub fn iter_chunk_sets_mut(&mut self) -> ChunkSetIter<'a> {
        ChunkSetIter::new(self.transaction, &self.transaction.archetypes)
    }

    /// Create a parallel iterator over the `ChunkSet`s in this transaction.
    pub fn par_iter_chunk_sets_mut(&mut self) -> ChunkSetParIter<'a> {
        ChunkSetParIter::new(self.transaction, &self.transaction.archetypes)
    }

    /// Iterate over all `Chunk`s in this transaction.
    pub fn iter_chunks_mut(&mut self) -> impl Iterator<Item=ChunkGuard<'a>> {
        self.iter_chunk_sets_mut()
            .flat_map(|mut chunk_set| chunk_set.iter_chunks_mut())
    }

    /// Iterate over all `Chunk`s in this transaction in parallel.
    pub fn par_iter_chunks_mut(&mut self) -> impl ParallelIterator<Item=ChunkGuard<'a>> {
        self.par_iter_chunk_sets_mut()
            .flat_map(|mut chunk_set| chunk_set.par_iter_chunks_mut())
    }
}

/// An iterator over `ChunkSet`s in a transaction.
pub struct ChunkSetIter<'a> {
    transaction: &'a Transaction,
    slice: &'a [Arc<Archetype>],
    offset: usize,
}

impl<'a> ChunkSetIter<'a> {
    pub(crate) fn new(transaction: &'a Transaction, slice: &'a [Arc<Archetype>]) -> ChunkSetIter<'a> {
        ChunkSetIter {
            transaction,
            slice,
            offset: 0,
        }
    }
}

impl<'a> Iterator for ChunkSetIter<'a> {
    type Item = ChunkSetGuard<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.slice.get(self.offset).map(|archetype| {
            self.offset += 1;

            let chunk_set = self.transaction.snapshot.chunk_set(archetype).unwrap();
            let archetype = archetype.clone();

            ChunkSetGuard::new(self.transaction, archetype, chunk_set)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.slice.len() - self.offset;
        (len, Some(len))
    }
}

impl<'a> DoubleEndedIterator for ChunkSetIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.offset == 0 {
            return None;
        }

        self.offset -= 1;
        self.slice.get(self.offset).map(|archetype| {
            let chunk_set = self.transaction.snapshot.chunk_set(archetype).unwrap();
            let archetype = archetype.clone();

            ChunkSetGuard::new(self.transaction, archetype, chunk_set)
        })
    }
}

impl<'a> ExactSizeIterator for ChunkSetIter<'a> {}

/// A parallel iterator over `ChunkSet`s in a transaction.
pub struct ChunkSetParIter<'a> {
    transaction: &'a Transaction,
    slice: &'a [Arc<Archetype>],
}

impl<'a> ChunkSetParIter<'a> {
    pub(crate) fn new(transaction: &'a Transaction, slice: &'a [Arc<Archetype>]) -> ChunkSetParIter<'a> {
        ChunkSetParIter {
            transaction,
            slice,
        }
    }
}

impl<'a> ParallelIterator for ChunkSetParIter<'a> {
    type Item = ChunkSetGuard<'a>;

    fn drive_unindexed<C>(self, consumer: C) -> <C as Consumer<Self::Item>>::Result where
        C: UnindexedConsumer<Self::Item> {
        bridge(self, consumer)
    }
}

impl<'a> IndexedParallelIterator for ChunkSetParIter<'a> {
    fn len(&self) -> usize {
        self.slice.len()
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> <C as Consumer<Self::Item>>::Result {
        bridge(self, consumer)
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> <CB as ProducerCallback<Self::Item>>::Output {
        callback.callback(ChunkSetProducer::new(self.transaction, self.slice))
    }
}

/// A producer for iterating over chunk sets in a transaction in parallel.
pub(crate) struct ChunkSetProducer<'a> {
    transaction: &'a Transaction,
    slice: &'a [Arc<Archetype>],
}

impl<'a> ChunkSetProducer<'a> {
    /// Create a new `ChunkSetProducer`.
    pub fn new(transaction: &'a Transaction, slice: &'a [Arc<Archetype>]) -> ChunkSetProducer<'a> {
        ChunkSetProducer {
            transaction,
            slice,
        }
    }
}

impl<'a> Producer for ChunkSetProducer<'a> {
    type Item = ChunkSetGuard<'a>;
    type IntoIter = ChunkSetIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ChunkSetIter::new(self.transaction, self.slice)
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let (left, right) = self.slice.split_at(index);
        let left = ChunkSetProducer::new(self.transaction, left);
        let right = ChunkSetProducer::new(self.transaction, right);
        (left, right)
    }
}
