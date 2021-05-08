use std::sync::Arc;

use chunked::{CommandBuffer, component, Snapshot, Universe};

#[derive(Debug, Clone, Copy, Default)]
pub struct MyComponent(i32);

component!(MyComponent);

fn main() {
    let universe = Universe::new();

    let mut command_buffer = CommandBuffer::new();
    let entity = universe.allocate_entity();
    command_buffer.set_component(entity, &MyComponent(3));

    let mut snapshot = Arc::new(Snapshot::empty(universe));
    snapshot.modify(command_buffer.iter_edits());

    println!("snapshot: {:?}", snapshot);
    println!("entity: {:?}", entity);

    let entity_reader = snapshot.entity(entity).unwrap();

    for component in entity_reader.component_types().as_slice() {
        println!("component: {:?}", component);
    }
}
