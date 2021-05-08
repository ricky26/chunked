[![Documentation](https://docs.rs/chunked/badge.svg)](https://docs.rs/chunked/)
[![Crates.io](https://img.shields.io/crates/v/chunked.svg)](https://crates.io/crates/chunked)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

# Chunked ECS
This repository houses the Chunked ECS.

# What is it?
Entity Component Systems model data and behaviour separately and are commonly
used for games. One of the benefits of an ECS is that you can isolate
individual behaviours (increasing reuse) and manage components en masse
(which can lead to some pretty nice optimisations).

# Why should I use Chunked?
Honestly, at this point, you probably shouldn't. It's essentially just a toy
project. However, if you're interested anyway:

- Chunked organises like entities into chunks, rather than organising by
  component, or by entity. This can reduce allocation load and still allow
  the benefits of structures-of-arrays for parallel compute.
  
- All components must implement `Copy`, which makes updating chunks require
  very little processing.
  
- All operations on the world are done via snapshots, which are copied on write.
  This allows you to take snapshots.
  
  This can be used, for example, to copy the latest snapshot to the rendering
  thread between updates, allowing for a blocking-free rendering thread.
  
- Worlds are exceptionally cheap to create.
  
  Worlds are a wrapper around a current snapshot. Archetypes and chunk free
  lists are stored in a Universe. Universes are shared between snapshots and
  Worlds.
  
- World-level transactions can be done in parallel. Waiting for a transaction to
  be possible is done with async/await.
  
- [Rayon](https://github.com/rayon-rs/rayon) is supported and is recommended for
  entity updates.

# What does it look like?

```rust
#[derive(Debug, Clone, Copy, Default)]
pub struct MyComponent(i32);

component!(MyComponent);

fn main() {
  let universe = Universe::new();

  let mut command_buffer = CommandBuffer::new();
  let entity = universe.allocate_entity();
  command_buffer.set_component(entity, &MyComponent(3));

  let mut snapshot = Arc::new(Snapshot::empty(universe.clone()));
  snapshot.modify(command_buffer.iter_edits());

  println!("snapshot: {:?}", snapshot);
  println!("entity: {:?}", entity);

  let entity_reader = snapshot.entity(entity).unwrap();

  for component in entity_reader.component_types().as_slice() {
    println!("component: {:?}", component);
  }
}
```

# Why another ECS crate?
Instead of managing all components of a type together, this library sorts
entities of a similar 'shape' into fixed-size chunks.

This also crate implements a Copy-on-Write approach to the entire
world state, which means that you can take snapshots. The particular use-case
for this was rendering in a separate thread to running world updates.

To be honest, it was mostly just for the challenge. ;)

# Why _Rust_?
Rust is a very performant and ergonomic systems programming language.
As an ECS library designed for use in games, performance is key. Rust's safety
and ergonomics make it ideal for implementing the low-level systems needed in
games. Additionally, if it needed to be used in a higher-level language, it
would probably be better to expose a simpler abstraction.

# Project structure

 - `packages/chunked` - The core package imlpementing the ECS.
 - `examples/orbits` - An N-Body simulation example.

# License
This project is licensed under the [MIT license](LICENSE).
