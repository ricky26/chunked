//! Base Entity types.
//! 
//! All entities are built up of Components. In this crate all entities are issued
//! a unique ID (per world) and each component type is allocated a unique ID
//! (globally).

use std::alloc::Layout;
use std::cmp::Ord;
use std::fmt::Debug;

use crate::component::{Component, ComponentTypeID};

/// The ID of a single entity.
/// 
/// Entity IDs are unique per World. They are not unique across worlds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityID(usize);

unsafe impl Component for EntityID {
    fn type_id() -> ComponentTypeID {
        ComponentTypeID::new(0)
    }

    fn layout() -> Layout {
        Layout::new::<Self>()
    }
}

impl EntityID {
    /// Create a new EntityID given the inner unique ID.
    pub fn new(id: usize) -> EntityID {
        EntityID(id)
    }

    /// Return the inner unique ID.
    pub fn id(&self) -> usize {
        self.0
    }
}
