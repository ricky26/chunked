use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use arc_swap::ArcSwap;
use crate::sorted_vec::VecSet;
use crate::{Archetype, Zone};

pub struct Universe {
    pub(crate) archetypes: ArcSwap<VecSet<Arc<Archetype>>>,
    pub(crate) zones: ArcSwap<Vec<Arc<Zone>>>,

    pub(crate) next_entity_id: AtomicUsize,
    pub(crate) next_zone_id: AtomicUsize,
    pub(crate) max_chunk_size: usize,
}

impl Universe {
    /// Create a new universe with the default settings.
    pub fn new() -> Arc<Universe> {
        Universe::with_max_chunk_size(4096)
    }

    /// Create a new universe with a specified maximum number of entities per chunk.
    pub fn with_max_chunk_size(max_chunk_size: usize) -> Arc<Universe> {
        Arc::new(Universe {
            archetypes: ArcSwap::from_pointee(VecSet::new()),
            zones: ArcSwap::from_pointee(Vec::new()),

            next_zone_id: AtomicUsize::new(1),
            next_entity_id: AtomicUsize::new(1),
            max_chunk_size,
        })
    }
}
