//! An entity component system.

pub use archetype::Archetype;
pub use chunk::Chunk;
pub use component::{
    Component,
    ComponentTypeID,
};
pub use entity::EntityID;
pub use snapshot::Snapshot;
pub use universe::Universe;
pub use world::World;

pub use command_buffer::CommandBuffer;
pub use system::{
    System,
    BoxSystem,
    SystemGroup,
};

pub mod component;
pub mod component_data;
mod entity;
pub mod archetype;

pub mod universe;
pub mod chunk;
pub mod chunk_set;
pub mod snapshot;

mod command_buffer;

pub mod world;
pub mod system;
