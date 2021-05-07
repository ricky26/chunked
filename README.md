# ECS
This repository houses my ECS implementation and associated examples.

# What is it?
Entity Component Systems model data and behaviour separately and are commonly
used for games. One of the nice benefits of ECS is that you can isolate
individual behaviours (increasing reuse) and manage components en masse in
behaviours (which can lead to some pretty nice optimisations).

# Why another ECS crate?
Mostly just for the challenge. ;)

# Why _Rust_?
Rust is a very good systems programming language. Generally when writing games
I like to separate the systems-level stuff and the actual data-driven
gamey-wamey bit. Rust works well where efficiency is needed in games systems
too.

# License
This project is licensed under the [MIT license](LICENSE).