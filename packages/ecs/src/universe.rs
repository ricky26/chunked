//! A universe is the container which `World`s exist inside.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::{Arc, atomic, RwLock};
use std::sync::atomic::AtomicUsize;

use crate::{Archetype, EntityID};
use crate::archetype::{ComponentSet, ComponentSetExt, ComponentSliceSet};

#[derive(Clone)]
enum ArchetypeSetEntry<'a> {
    Entry(Arc<Archetype>),
    Key(ComponentSliceSet<'a>),
}

impl<'a> ArchetypeSetEntry<'a> {
    fn do_cmp_inner<T: ComponentSet>(&self, b: &T) -> Ordering {
        match self {
            ArchetypeSetEntry::Entry(ref e) => e.component_types().cmp(b),
            ArchetypeSetEntry::Key(ref k) => k.cmp(b),
        }
    }

    fn do_cmp(&self, b: &ArchetypeSetEntry<'a>) -> Ordering {
        // This is slightly horrific because trait references are cursed.

        match b {
            ArchetypeSetEntry::Entry(ref e) => self.do_cmp_inner(e.component_types()),
            ArchetypeSetEntry::Key(ref k) => self.do_cmp_inner(k),
        }
    }
}

impl<'a> PartialEq for ArchetypeSetEntry<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.do_cmp(other) == Ordering::Equal
    }
}

impl<'a> Eq for ArchetypeSetEntry<'a> {}

impl<'a> PartialOrd for ArchetypeSetEntry<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.do_cmp(other))
    }
}

impl<'a> Ord for ArchetypeSetEntry<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.do_cmp(other)
    }
}

struct UniverseSync {
    archetypes: Vec<Arc<Archetype>>,
    archetype_lookup: BTreeSet<ArchetypeSetEntry<'static>>,
}


/// A universe contains all of the globals which can be shared between worlds.
///
/// This includes things like chunk allocation zones and the archetype list.
pub struct Universe {
    chunk_size: usize,

    next_entity_id: AtomicUsize,

    sync: RwLock<UniverseSync>,
}

impl Universe {
    /// Create a new universe with the default settings.
    pub fn new() -> Arc<Universe> {
        Universe::with_chunk_size(1024)
    }

    /// Create a new universe with a specified maximum number of entities per chunk.
    pub fn with_chunk_size(chunk_size: usize) -> Arc<Universe> {
        let sync = UniverseSync {
            archetypes: Vec::new(),
            archetype_lookup: BTreeSet::new(),
        }.into();

        Arc::new(Universe {
            chunk_size,
            next_entity_id: AtomicUsize::new(1),
            sync,
        })
    }

    /// Get the chunk size in number of entities per chunk.
    pub fn chunk_size(&self) -> usize { self.chunk_size }

    /// Fetch an archetype by its ID.
    pub fn archetype_by_id(&self, id: usize) -> Option<Arc<Archetype>> {
        let rd = self.sync.read().unwrap();
        rd.archetypes.get(id).cloned()
    }

    /// Get a archetype with the given component types, providing it already
    /// exists in the world.
    ///
    /// Generally, one should prefer `ensure_archetype` unless you have a
    /// particular reason to not want the archetype to be created, as it handles
    /// the fast case of the archetype already existing.
    pub fn archetype<'a, T: ComponentSet>(&self, component_types: T) -> Option<Arc<Archetype>> {
        let component_types = component_types.as_ref();
        let key = ArchetypeSetEntry::Key(component_types);

        let rd = self.sync.read().unwrap();
        rd.archetype_lookup.get(&key).cloned().map(|x| {
            match x {
                ArchetypeSetEntry::Entry(k) => k,
                ArchetypeSetEntry::Key(_) => panic!("key in archetype set"),
            }
        })
    }

    /// Get the archetype which contains exactly the requested component types.
    ///
    /// The archetype might not contain the given types in the order requested,
    /// they will be sorted by their unique ID.
    pub fn ensure_archetype<'a>(self: &Arc<Universe>, component_types: impl ComponentSet) -> Arc<Archetype> {
        let component_types = component_types.into_owned();

        // Optimistically try just a read lock first.
        if let Some(existing) = self.archetype(component_types.as_ref()) {
            return existing;
        }

        let mut wr = self.sync.write().unwrap();
        let id = wr.archetypes.len();
        let new_archetype =
            Arc::new(Archetype::new(self, component_types, id));
        wr.archetypes.push(new_archetype.clone());
        wr.archetype_lookup.insert(ArchetypeSetEntry::Entry(new_archetype.clone()));
        new_archetype
    }

    /// Generate a new entity ID which is unique for this world.
    pub fn allocate_entity(&self) -> EntityID {
        EntityID::new(self.next_entity_id.fetch_add(1, atomic::Ordering::Relaxed))
    }

    /// Flush all cached memory.
    pub fn flush(&self) {
        let rd = self.sync.read().unwrap();
        for arch in rd.archetypes.iter() {
            arch.flush();
        }
    }
}
