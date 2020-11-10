//! A snapshot implementation which snapshots the state of a World.

use std::ops::{Deref};
use std::fmt::{self, Debug};
use std::sync::Arc;

pub use reader::EntityReader;
pub use writer::{SnapshotWriter, SnapshotWriterGuard};

mod reader;
mod writer;

#[cfg(feature = "rayon")]
use rayon::iter::{
    ParallelIterator,
    IntoParallelRefIterator,
};

use crate::entity::EntityID;
use crate::universe::Universe;
use crate::chunk::{Chunk, ChunkSet};

/// A snapshot of the state of the world.
#[derive(Clone)]
pub struct Snapshot {
    universe: Arc<Universe>,
    pub(crate) chunk_sets: Vec<ChunkSet>,
}

impl Snapshot {
    /// Create a new snapshot of an empty world.
    pub fn empty(universe: Arc<Universe>) -> Snapshot {
        Snapshot {
            universe,
            chunk_sets: Vec::new(),
        }
    }

    /// Get a weak reference to the owning universe of this snapshot.
    pub fn universe(&self) -> &Arc<Universe> {
        &self.universe
    }

    /// Create a parallel iterator over the chunks in the snapshot.
    #[cfg(feature = "rayon")]
    pub fn par_iter_chunks(&self) -> impl ParallelIterator<Item=&Arc<Chunk>> {
        self.chunk_sets.par_iter().flat_map(|cs| cs.chunks())
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
