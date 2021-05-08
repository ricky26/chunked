//! Collections of chunks with the same archetype.

use std::convert::TryFrom;
use std::sync::Arc;

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{Archetype, Chunk, EntityID};
use crate::chunk::{ChunkAction, ChunkEdit, ChunkEntityData};
use crate::component_data::{ComponentDataSlice, ComponentValueRef};
use std::ops::Deref;

fn chunk_min(c: &Chunk) -> EntityID {
    *c.components::<EntityID>().unwrap().first().unwrap()
}

fn chunk_max(c: &Chunk) -> EntityID {
    *c.components::<EntityID>().unwrap().last().unwrap()
}

/// A sorted collection of `Chunk`s which all refer to the same `Archetype`.
///
/// This structure is used for efficient lookup and modification of multiple
/// chunks.
#[derive(Debug, Clone)]
pub struct ChunkSet {
    storage: Vec<Arc<Chunk>>,
}

impl ChunkSet {
    /// Create a new empty chunk set.
    pub fn new() -> ChunkSet {
        ChunkSet {
            storage: Vec::new(),
        }
    }

    /// Create a new chunk set with a given capacity.
    pub fn with_capacity(capacity: usize) -> ChunkSet {
        ChunkSet {
            storage: Vec::with_capacity(capacity),
        }
    }

    /// Get the number of chunks in this set.
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    /// Return true if this chunk set is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the list of chunks which are members of this `ChunkSet`.
    pub fn chunks(&self) -> &[Arc<Chunk>] {
        &self.storage
    }

    /// Lookup the index of the chunk which may contain the given entity.
    pub fn chunk_index_for_entity(&self, id: EntityID) -> Option<usize> {
        if self.storage.is_empty() {
            None
        } else {
            let idx = match self.storage.binary_search_by_key(&id, |c| {
                *c.components::<EntityID>().unwrap().first().unwrap()
            }) {
                Ok(idx) => idx,
                Err(idx) => idx.min(1) - 1,
            };
            Some(idx)
        }
    }

    /// Lookup the chunk which may contain an entity with the given ID.
    ///
    /// This may return `Some` even if the given chunk does not actually contain
    /// the entity with the given ID. However, it does return the only chunk
    /// which could contain it.
    pub fn chunk_for_entity(&self, id: EntityID) -> Option<&Arc<Chunk>> {
        let idx = self.chunk_index_for_entity(id);
        idx.map(|idx| &self.storage[idx])
    }

    /// Lookup a mutable reference to the chunk which may contain an entity
    /// with the given ID.
    pub fn chunk_for_entity_mut(&mut self, id: EntityID) -> Option<&mut Arc<Chunk>> {
        if let Some(idx) = self.chunk_index_for_entity(id) {
            Some(&mut self.storage[idx])
        } else {
            None
        }
    }

    /// Lookup the index of the chunk which may contain an entity, starting with a given hint index.
    ///
    /// Hint may be used to speed up searching if the entity is nearby.
    pub fn chunk_index_for_entity_hint(&self, id: EntityID, hint: usize) -> Option<usize> {
        let chunk_a = self.storage.get(hint);

        // Optimistically check whether the entity is in the next two chunks.
        if chunk_a.map_or(false, |c| chunk_min(c) <= id) {
            let chunk_a = chunk_a.unwrap();
            if chunk_max(chunk_a) >= id {
                return Some(hint);
            }

            if let Some(chunk_b) = self.storage.get(hint + 1) {
                if chunk_max(chunk_b) >= id {
                    return Some(hint + 1);
                }
            }
        }

        if hint == 0 {
            None
        } else {
            self.chunk_index_for_entity(id)
        }
    }

    /// Create a parallel iterator over the chunks in this set.
    pub fn par_iter_chunks(&self) -> impl ParallelIterator<Item=&Arc<Chunk>> {
        self.storage.par_iter()
    }

    /// Create an iterator over all the chunks in this set.
    pub fn iter_chunks(&self) -> impl Iterator<Item=&Arc<Chunk>> {
        self.storage.iter()
    }

    /// Re-assert the invariant: all chunks should contain between N/2 and N
    /// entities (so long as there are 2 or more chunks).
    fn rebalance(&mut self) {
        if self.storage.len() < 2 {
            return;
        }

        let mut split_idx = 1;
        while split_idx < self.storage.len() {
            let (chunk_a, chunk_b) = self.storage.split_at_mut(split_idx);
            let chunk_a = chunk_a.last_mut().unwrap();
            let chunk_b = chunk_b.first_mut().unwrap();

            let cap = chunk_a.capacity();
            let half_cap = cap >> 1;
            let a_len = chunk_a.len();
            let b_len = chunk_b.len();

            if a_len + b_len < cap {
                // The two chunks can be very simply merged.
                let chunk_a = Arc::make_mut(chunk_a);
                chunk_a.copy_from(chunk_a.len(), chunk_b, ..);
                self.storage.remove(split_idx);
            } else if a_len < half_cap {
                // A is too small.
                let chunk_a = Arc::make_mut(chunk_a);
                let chunk_b = Arc::make_mut(chunk_b);
                let split_at = (a_len + b_len) >> 1;
                let to_move = split_at - a_len;
                chunk_a.copy_from(a_len, chunk_b, ..to_move);
                chunk_b.remove(..to_move);
            } else if b_len < half_cap {
                // B is too small.
                let chunk_a = Arc::make_mut(chunk_a);
                let chunk_b = Arc::make_mut(chunk_b);
                let split_at = (a_len + b_len) >> 1;
                chunk_b.copy_from(0, chunk_a, split_at..);
                chunk_a.remove(split_at..);
            } else {
                // Both sides are N/2 <= len <= N
                split_idx += 1;
            }
        }
    }

    /// Apply an edit list to this chunk.
    pub(crate) fn modify<'a>(&mut self,
                             archetype: Arc<Archetype>,
                             mut edits: Vec<ChunkEdit>,
                             component_data: &[ComponentValueRef<'a>]) {
        // This is needed so we can optimally resize single chunks later on.
        edits.sort_by_key(|e| e.0);

        let mut last_chunk_index = 0;
        let mut chunk_edit_start = 0;

        while chunk_edit_start < edits.len() {
            let ChunkEdit(first_id, _) = edits[chunk_edit_start];

            let chunk_idx = self.chunk_index_for_entity_hint(first_id, last_chunk_index);
            let mut chunk_idx = if let Some(chunk_idx) = chunk_idx {
                chunk_idx
            } else {
                self.storage.push(Arc::new(archetype.clone().new_chunk()));
                0
            };
            last_chunk_index = chunk_idx;

            let max_entity_id = self.storage.get(chunk_idx + 1)
                .and_then(|c| c.components::<EntityID>().unwrap().first())
                .copied();
            let edit_chunk = &mut self.storage[chunk_idx];

            // Find all the entries that would go in this chunk.
            let chunk_edit_end = if let Some(max_id) = max_entity_id {
                let mut end = chunk_edit_start;
                while end < edits.len() {
                    let id = edits[end].0;
                    if id >= max_id {
                        break;
                    }
                    end += 1;
                }
                end
            } else {
                edits.len()
            };

            let chunk_edits = &edits[chunk_edit_start..chunk_edit_end];
            let new_size = chunk_edits.iter()
                .fold(edit_chunk.len(), |acc, edit| {
                    let ChunkEdit(_, action) = edit;
                    match action {
                        ChunkAction::Upsert(_, _) => acc + 1,
                        ChunkAction::Remove => acc - 1,
                    }
                });

            if new_size == 0 {
                // We drained a chunk, remove it.
                self.storage.remove(chunk_idx);
            } else if new_size <= edit_chunk.capacity() {
                // If this makes up a single chunk, mutate the existing chunk.

                // Reverse iterate so we can resize optimally.
                let edit_chunk = Arc::make_mut(edit_chunk);
                edit_chunk.modify(chunk_edits.iter().rev(), component_data);
            } else {
                // It doesn't fit in one chunk, so just start streaming chunks until we've
                // applied all edits.
                // Unbalanced chunks will be balanced later.

                let old_chunk = self.storage.remove(chunk_idx);
                let entity_ids = old_chunk.components::<EntityID>().unwrap();
                let mut read_idx = 0;
                let mut edit_idx = 0;

                let mut new_chunk = archetype.clone().new_chunk();

                fn alloc_new_chunk(storage: &mut Vec<Arc<Chunk>>, archetype: &Arc<Archetype>, new_chunk: &mut Chunk, chunk_idx: &mut usize) {
                    if new_chunk.len() == new_chunk.capacity() {
                        let to_insert = Arc::new(std::mem::replace(
                            new_chunk,
                            archetype.clone().new_chunk()));
                        storage.insert(*chunk_idx, to_insert);
                        *chunk_idx += 1;
                    }
                }

                // Ad-hoc sorted merge.
                while read_idx < entity_ids.len() || edit_idx < chunk_edits.len() {
                    let old_id = entity_ids.get(read_idx).cloned();
                    let next_edit = chunk_edits.get(edit_idx).cloned();

                    match next_edit {
                        Some(ChunkEdit(id, action)) => {
                            match action {
                                ChunkAction::Upsert(data_start, data_end) => {
                                    alloc_new_chunk(&mut self.storage, &archetype, &mut new_chunk, &mut chunk_idx);

                                    let data_slice = &component_data[data_start..data_end];
                                    let data = ComponentDataSlice::try_from(data_slice).unwrap();

                                    if Some(id) == old_id {
                                        // If this is an update we have to apply the old components
                                        // first, then copy over the top to make sure we don't
                                        // lose any values.
                                        let write_idx = new_chunk.len();
                                        new_chunk.insert(
                                            write_idx,
                                            &ChunkEntityData::new(old_chunk.clone(), read_idx));
                                        new_chunk.update_at(write_idx, &data);
                                        read_idx += 1;
                                    } else {
                                        let write_idx = new_chunk.len();
                                        new_chunk.insert(write_idx, &data);
                                        new_chunk.components_mut::<EntityID>().unwrap()[write_idx] = id;
                                    }
                                }
                                ChunkAction::Remove => {
                                    assert_eq!(id, old_id.unwrap());
                                    read_idx += 1;
                                }
                            }

                            edit_idx += 1;
                        }
                        None => {
                            while read_idx < entity_ids.len() {
                                alloc_new_chunk(&mut self.storage, &archetype, &mut new_chunk, &mut chunk_idx);

                                let to_copy = (entity_ids.len() - read_idx)
                                    .min(new_chunk.capacity() - new_chunk.len());
                                new_chunk.copy_from(
                                    new_chunk.len(),
                                    &old_chunk,
                                    read_idx..read_idx+to_copy);
                                read_idx += to_copy;
                            }
                        }
                    }
                }

                self.storage.insert(chunk_idx, Arc::new(new_chunk));
            }

            chunk_edit_start = chunk_edit_end;
        }

        self.rebalance();
    }
}

impl Default for ChunkSet {
    fn default() -> Self {
        ChunkSet::new()
    }
}

impl Deref for ChunkSet {
    type Target = [Arc<Chunk>];

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}
