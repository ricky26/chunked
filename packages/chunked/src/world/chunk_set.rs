use std::sync::Arc;

use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use rayon::iter::plumbing::{bridge, Consumer, Producer, ProducerCallback, UnindexedConsumer};

use crate::{Archetype, Chunk};
use crate::chunk_set::ChunkSet;
use crate::world::ChunkGuard;
use crate::world::transaction::Transaction;

/// A guard that holds a `ChunkSet` read-write lock.
pub struct ChunkSetGuard<'a> {
    transaction: &'a Transaction,
    archetype: Arc<Archetype>,
    chunk_set: &'a mut ChunkSet,
}

impl<'a> ChunkSetGuard<'a> {
    /// Create a new `ChunkSetGuard`.
    pub(crate) fn new(
        transaction: &'a Transaction,
        archetype: Arc<Archetype>,
        chunk_set: &'a mut ChunkSet,
    ) -> ChunkSetGuard<'a> {
        ChunkSetGuard {
            transaction,
            archetype,
            chunk_set,
        }
    }


    /// Get the archetype this `ChunkSet` belongs to.
    pub fn archetype(&self) -> &Arc<Archetype> {
        &self.archetype
    }

    /// Get the number of chunks in this set.
    pub fn len(&self) -> usize {
        self.chunk_set.len()
    }

    /// Returns true if this chunk set contains no chunks.
    pub fn is_empty(&self) -> bool {
        self.chunk_set.is_empty()
    }

    /// Fetch a single `Chunk` from this guard.
    pub fn chunk_mut(&mut self, idx: usize) -> Option<ChunkGuard<'_>> {
        if let Some(chunk) = self.chunk_set.get_mut(idx) {
            Some(ChunkGuard::new(self.transaction, chunk))
        } else {
            None
        }
    }

    /// Iterate over all `Chunk`s in this guard.
    pub fn iter_chunks_mut(self) -> ChunkIter<'a> {
        ChunkIter::new(self.transaction, self.chunk_set.chunks_mut())
    }

    /// Iterate over all `Chunk`s in this guard, in parallel.
    pub fn par_iter_chunks_mut(self) -> ChunkParIter<'a> {
        ChunkParIter::new(self.transaction, self.chunk_set.chunks_mut())
    }
}

/// An iterator over all of the chunks in a `ChunkSetGuard`.
pub struct ChunkIter<'a> {
    transaction: &'a Transaction,
    slice: &'a mut [Arc<Chunk>],
    offset: usize,
}

impl<'a> ChunkIter<'a> {
    // Create a new `ChunkIter`.
    pub(crate) fn new(
        transaction: &'a Transaction,
        slice: &'a mut [Arc<Chunk>],
    ) -> ChunkIter<'a> {
        ChunkIter {
            transaction,
            slice,
            offset: 0,
        }
    }
}

impl<'a> Iterator for ChunkIter<'a> {
    type Item = ChunkGuard<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(chunk) = self.slice.get_mut(self.offset) {
            self.offset += 1;
            Some(ChunkGuard::new(self.transaction, chunk))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.slice.len() - self.offset;
        (len, Some(len))
    }
}

impl<'a> DoubleEndedIterator for ChunkIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.offset == 0 {
            return None;
        }

        self.offset -= 1;
        if let Some(chunk) = self.slice.get_mut(self.offset) {
            Some(ChunkGuard::new(self.transaction, chunk))
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for ChunkIter<'a> {}


/// A parallel iterator over all chunks in a `ChunkSetGuard`.
pub struct ChunkParIter<'a> {
    transaction: &'a Transaction,
    slice: &'a mut [Arc<Chunk>],
}

impl<'a> ChunkParIter<'a> {
    pub(crate) fn new(
        transaction: &'a Transaction,
        slice: &'a mut [Arc<Chunk>],
    ) -> ChunkParIter<'a> {
        ChunkParIter {
            transaction,
            slice,
        }
    }
}

impl<'a> ParallelIterator for ChunkParIter<'a> {
    type Item = ChunkGuard<'a>;

    fn drive_unindexed<C>(self, consumer: C) -> <C as Consumer<Self::Item>>::Result where
        C: UnindexedConsumer<Self::Item> {
        bridge(self, consumer)
    }
}

impl<'a> IndexedParallelIterator for ChunkParIter<'a> {
    fn len(&self) -> usize {
        self.slice.len()
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> <C as Consumer<Self::Item>>::Result {
        bridge(self, consumer)
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> <CB as ProducerCallback<Self::Item>>::Output {
        callback.callback(ChunkProducer::new(self.transaction, self.slice))
    }
}

/// A producer for iterating over chunks in a set in parallel.
pub(crate) struct ChunkProducer<'a> {
    transaction: &'a Transaction,
    slice: &'a mut [Arc<Chunk>],
}

impl<'a> ChunkProducer<'a> {
    /// Create a new `ChunkProducer`.
    pub fn new(transaction: &'a Transaction, slice: &'a mut [Arc<Chunk>]) -> ChunkProducer<'a> {
        ChunkProducer {
            transaction,
            slice,
        }
    }
}

impl<'a> Producer for ChunkProducer<'a> {
    type Item = ChunkGuard<'a>;
    type IntoIter = ChunkIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ChunkIter::new(self.transaction, self.slice)
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let (left, right) = self.slice.split_at_mut(index);
        let left = ChunkProducer::new(self.transaction, left);
        let right = ChunkProducer::new(self.transaction, right);
        (left, right)
    }
}
