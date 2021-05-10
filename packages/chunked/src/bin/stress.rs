use chunked::{CommandBuffer, component, ModifySnapshot, Snapshot, Universe};

#[derive(Debug, Clone, Copy, Default)]
pub struct MyComponent(i32);

component!(MyComponent);

fn main() {
    let universe = Universe::new();
    let mut snapshot = Snapshot::empty(universe.clone());
    let mut to_delete = Vec::new();

    for _ in 0..8 {
        let mut command_buffer = CommandBuffer::new();

        for id in to_delete.drain(..) {
            command_buffer.remove_component::<MyComponent>(id);
        }

        for idx in 0..512 {
            let entity_id = universe.allocate_entity();
            command_buffer.set_component(entity_id, &MyComponent(32));

            if idx % 12 == 11 {
                to_delete.push(entity_id);
            }
        }

        snapshot.modify(command_buffer.iter_edits());
    }

    println!("snapshot: {:?}", snapshot);
}
