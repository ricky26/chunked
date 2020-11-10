use std::sync::Arc;
use crate::snapshot::Snapshot;
use crate::{EntityID, Archetype, EntityComponentIterator, Component};

/// A reader for retreiving single entities from a snapshot.
pub struct EntityReader {
    pub(crate) snapshot: Arc<Snapshot>,
    pub(crate) chunk_set_index: usize,
    pub(crate) chunk_index: usize,
    pub(crate) entity_index: usize,
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
        let chunk = &chunk_set.chunks()[self.chunk_index];

        chunk.get_components::<T>()
            .and_then(|slice| slice.get(self.entity_index))
    }
}
