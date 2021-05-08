use std::sync::Arc;

use chunked::{CommandBuffer, component, Universe, Snapshot};

#[derive(Debug, Clone, Copy, Default)]
pub struct MyComponent(i32);

component!(MyComponent);

fn main() {
    let universe = Universe::new();
    let mut snapshot = Arc::new(Snapshot::empty(universe.clone()));

    for _ in 0..8 {
        let mut command_buffer = CommandBuffer::new();

        for _ in 0..512 {
            let entity_id = universe.allocate_entity();
            command_buffer.set_component(entity_id, &MyComponent(32));
        }

        snapshot.modify(command_buffer.iter_edits());
    }

    println!("snapshot: {:?}", snapshot);
}
