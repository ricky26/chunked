use std::sync::Arc;
use futures::executor::block_on;
use ecs::{
    component,
    ComponentType,
    World,
    CommandBuffer,
};

#[derive(Debug,Clone,Copy,Default)]
pub struct MyComponent(i32);

component!(MyComponent);

fn main() {
    let world = Arc::new(World::new());
    let arch = world.ensure_archetype(vec![
        ComponentType::for_type::<MyComponent>(),
    ]);

    for _ in 0..8 {
        let mut command_buffer = CommandBuffer::new(world.clone());

        for _ in 0..512 {
            command_buffer.new_entity(arch.clone());
        }

        block_on(command_buffer.execute());
    }

    println!("world: {:?}", world);
}
