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
pub use chunk::{
    Zone,
    Chunk,
    ChunkSet,
};
pub use world::{
    World,
    CommandBuffer,
    Snapshot,
    SnapshotWriter,
};
pub use system::{  
    ChunkSystem,
    SnapshotSystem,
    SystemRegistration,
    SystemSet,
};

mod sorted_vec;
pub mod entity;
pub mod chunk;
pub mod world;
pub mod system;
