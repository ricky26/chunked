use std::sync::Arc;
use futures::executor::block_on;
use ecs::{
    component,
    ComponentType,
    Universe,
    World,
    CommandBuffer,
};

#[derive(Debug,Clone,Copy,Default)]
pub struct MyComponent(i32);

component!(MyComponent);

fn main() {
    let universe = Universe::new();
    let world = Arc::new(World::new(universe));
    let arch = world.ensure_archetype(vec![
        ComponentType::for_type::<MyComponent>(),
    ]);

    let mut command_buffer = CommandBuffer::new(world.clone());
    let entity = command_buffer.new_entity(arch);
    block_on(command_buffer.execute());

    println!("world: {:?}", world);
    println!("entity: {:?}", entity);

    let snapshot = world.snapshot();
    let entity_reader = snapshot.entity_reader(entity).unwrap();

    for component in entity_reader.component_types() {
        println!("component: {:?}", component);
    }
}
