use std::sync::Arc;
use std::sync;
use std::ops::{Deref, DerefMut};
use bit_vec::BitVec;
use rayon::iter::{ParallelIterator, IntoParallelIterator};
use futures::channel::oneshot;

use crate::{ChunkSet, Chunk};
use super::Snapshot;

enum SnapshotWriterState {
    Original(Arc<Snapshot>),
    Replace(Arc<Snapshot>),
    ChunkWise {
        snapshot: Arc<Snapshot>,
        locked: BitVec,
    },
}

impl SnapshotWriterState {
    /// Get the current snapshot stored by this state.
    pub fn snapshot(&self) -> &Arc<Snapshot> {
        match self {
            &SnapshotWriterState::Original(ref snapshot) => snapshot,
            &SnapshotWriterState::Replace(ref snapshot) => snapshot,
            &SnapshotWriterState::ChunkWise { ref snapshot, .. } => snapshot,
        }
    }
}

struct SnapshotWriterInner {
    num_chunks: usize,
    contents: sync::Mutex<SnapshotWriterState>,
    cond: sync::Condvar,
    drop_tx: Option<oneshot::Sender<Arc<Snapshot>>>
}

impl Drop for SnapshotWriterInner {
    fn drop(&mut self) {
        if let Some(tx) = self.drop_tx.take() {
            let mut contents = self.contents.lock().unwrap();
            let snap = contents.snapshot().clone();
            let empty_snap = Arc::new(Snapshot::empty(snap.universe().clone()));
            *contents = SnapshotWriterState::Original(empty_snap);
            tx.send(snap).ok();
        }
    }
}

/// A writer for modifying snapshots.
///
/// This can be used in one of two ways:
/// - As a simple `Snapshot` reference.
/// - As a chunk-wise writer (which allows parallel writing).
#[derive(Clone)]
pub struct SnapshotWriter {
    inner: Arc<SnapshotWriterInner>,
}

impl SnapshotWriter {
    /// Create a new `SnapshotWriter` with the given starting state.
    pub fn new(snapshot: Arc<Snapshot>) -> (SnapshotWriter, oneshot::Receiver<Arc<Snapshot>>) {
        let (drop_tx, drop_rx) = oneshot::channel();
        let num_chunks = snapshot.chunk_sets.iter()
            .map(|cs| cs.chunks().len())
            .sum();

        let inner = Arc::new(SnapshotWriterInner {
            num_chunks,
            contents: sync::Mutex::new(SnapshotWriterState::Original(snapshot)),
            cond: sync::Condvar::new(),
            drop_tx: Some(drop_tx),
        });

        (SnapshotWriter { inner }, drop_rx)
    }

    /// Get the snapshot this writer currently holds.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        let contents = self.inner.contents.lock().unwrap();
        contents.snapshot().clone()
    }

    /// Return the number of chunks covered by this writer.
    pub fn num_chunks(&self) -> usize {
        self.inner.num_chunks
    }

    /// Set the final snapshot directly.
    pub fn set_snapshot(&self, snapshot: Arc<Snapshot>) {
        let mut contents = self.inner.contents.lock().unwrap();

        // Take exclusive lock!
        loop {
            match &mut *contents {
                &mut SnapshotWriterState::Original(_) |
                &mut SnapshotWriterState::Replace(_) => {},
                &mut SnapshotWriterState::ChunkWise {ref locked, .. } => {
                    if locked.any() {
                        contents = self.inner.cond.wait(contents).unwrap();
                        continue;
                    }

                    break;
                },
            };

            break;
        }

        *contents = SnapshotWriterState::Replace(snapshot);
    }

    /// Create a parallel iterator over all chunks with mutable references.
    #[cfg(feature = "rayon")]
    pub fn par_iter_chunks_mut(&self) -> impl ParallelIterator<Item=SnapshotWriterGuard> + '_ {
        (0..self.inner.num_chunks).into_par_iter().map(move |idx| self.borrow_chunk_mut(idx))
    }

    /// Iterate through all chunks with mutable references.
    pub fn iter_chunks_mut(&self) -> impl Iterator<Item=SnapshotWriterGuard> + '_ {
        (0..self.inner.num_chunks).map(move |idx| self.borrow_chunk_mut(idx))
    }

    /// Borrow a single chunk mutably.
    ///
    /// This method cannot be used if `set_snapshot` has been called.
    pub fn borrow_chunk_mut(&self, index: usize) -> SnapshotWriterGuard {
        let (chunk_set, chunk_index) = loop {
            let mut contents = self.inner.contents.lock().unwrap();

            let (snapshot, locked) = match &mut *contents {
                &mut SnapshotWriterState::Original(ref snapshot) => {
                    // Starting chunk-wise lock.
                    *contents = SnapshotWriterState::ChunkWise {
                        snapshot: snapshot.clone(),
                        locked: BitVec::from_elem(self.inner.num_chunks, false),
                    };
                    continue;
                },
                &mut SnapshotWriterState::Replace(_) => {
                    panic!("tried to use chunk-wise modification after replacing snapshot");
                },
                &mut SnapshotWriterState::ChunkWise { ref mut snapshot, ref mut locked } =>
                    (snapshot, locked),
            };

            let (chunk_set_index, chunk_index) = {
                let mut chunk_index = index;
                let mut chunk_set_index = 0;

                for chunk_set in snapshot.chunk_sets.iter() {
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

            let index = snapshot.chunk_sets.iter()
                .map(|cs| cs.chunks().len())
                .take(chunk_set_index)
                .sum::<usize>() + chunk_index;

            if locked[index] {
                let _ = self.inner.cond.wait(contents).unwrap();
            } else {
                locked.set(index, true);
                let snapshot_mut = Arc::make_mut(snapshot);
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
        let mut contents = self.inner.contents.lock().unwrap();

        match &mut *contents {
            &mut SnapshotWriterState::ChunkWise { ref mut locked, .. } =>
                locked.set(index, false),
            _ => unreachable!(),
        }

        self.inner.cond.notify_all();
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
