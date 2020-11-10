//! An entity component system.
//! 
//! This crate is based on the design of Unity DoTS.

pub use entity::{
    Component,
    ComponentType,
    ComponentTypeID,
    Archetype,
    EntityComponentIterator,
    EntityID,
};
pub use universe::Universe;
pub use chunk::{
    Zone,
    Chunk,
    ChunkSet,
};
pub use snapshot::{
    Snapshot,
    SnapshotWriter,
    SnapshotWriterGuard,
};
pub use world::{
    World,
    CommandBuffer,
};
pub use system::{  
    System,
    SystemRegistration,
    SystemSet,
};

mod sorted_vec;
mod reusable;

pub mod universe;
pub mod chunk;
pub mod snapshot;

pub mod entity;
pub mod world;
pub mod system;
