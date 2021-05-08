//! Composable ECS systems.

use std::cmp::Ord;
use std::fmt::Debug;
use std::fmt::Display;

use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;

/// A token which represents a system in a `SystemSet`.
/// 
/// These tokens are not unique between `SystemSet`s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SystemID(pub u32);

/// System wraps a single system in
#[async_trait]
pub trait System {
    async fn update(&mut self);
}

#[async_trait]
impl<F> System for F
    where F: (Fn() -> BoxFuture<'static, ()>) + Send + Sync
{
    async fn update(&mut self) {
        (self)().await
    }
}

/// A registration used for building `SystemGroups`s.
pub struct BoxSystem {
    system: Box<dyn System + Send>,
    before: Vec<SystemID>,
    after: Vec<SystemID>,
}

impl BoxSystem {
    /// Create a new system from the given function.
    pub fn new<S: System + Send + 'static>(s: S) -> BoxSystem {
        BoxSystem {
            system: Box::new(s),

            before: Vec::new(),
            after: Vec::new(),
        }
    }

    /// Require that this system is updated before the system represented
    /// by the given token.
    pub fn before(mut self, system: SystemID) -> Self {
        if let Err(insert_idx) = self.before.binary_search(&system) {
            self.before.insert(insert_idx, system);
        }

        self
    }

    /// Require that this system is updated after the system represented
    /// by the given token.
    pub fn after(mut self, system: SystemID) -> Self {
        if let Err(insert_idx) = self.after.binary_search(&system) {
            self.after.insert(insert_idx, system);
        }

        self
    }

    /// Run one update of this system.
    pub fn update(&mut self) -> BoxFuture<()> {
        self.system.update()
    }
}

/// The error returned when the requirements for a system cannot be met.
#[derive(Clone, Debug)]
pub struct SystemRegistrationError;

impl Display for SystemRegistrationError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "conflicting system requirements")
    }
}

impl std::error::Error for SystemRegistrationError {}

/// A set of systems which are used to modify a world.
pub struct SystemGroup {
    next_system_id: u32,

    dependencies: Vec<SystemID>,
    system_dependencies: Vec<(usize, usize)>,
    systems: Vec<BoxSystem>,
}

impl SystemGroup {
    /// Create a new empty `SystemSet`.
    pub fn new() -> SystemGroup {
        SystemGroup {
            next_system_id: 0,

            dependencies: Vec::new(),
            system_dependencies: Vec::new(),
            systems: Vec::new(),
        }
    }

    fn calculate_dependencies(&mut self) {
        self.dependencies.clear();
        self.system_dependencies.clear();

        fn add_dep(deps: &mut Vec<SystemID>, offset: usize, id: SystemID) {
            if let Err(idx) = deps[offset..].binary_search(&id) {
                deps.insert(idx + offset, id);
            }
        }

        for (idx_a, system) in self.systems.iter().enumerate() {
            let id_a = SystemID(idx_a as u32);
            let start = self.dependencies.len();

            for dep in system.after.iter().copied() {
                if dep.0 >= self.systems.len() as u32 {
                    break;
                }

                add_dep(&mut self.dependencies, start, dep);
            }

            for (idx_b, system) in self.systems.iter().enumerate() {
                let id_b = SystemID(idx_b as u32);
                if idx_b == idx_a {
                    continue;
                }

                if system.before.binary_search(&id_a).is_ok() {
                    add_dep(&mut self.dependencies, start, id_b);
                }
            }

            self.system_dependencies.push((start, self.dependencies.len()));
        }
    }

    /// Insert a system to the set according to its requirements.
    pub fn insert(&mut self, system: BoxSystem) -> SystemID {
        let token = SystemID(self.next_system_id);
        self.next_system_id += 1;
        self.systems.push(system);
        self.calculate_dependencies();
        token
    }

    /// Run an update for every system.
    pub async fn update(&mut self) {
        let mut pending = self.systems.iter_mut()
            .enumerate()
            .map(|(idx, s)| (idx, s, 0))
            .collect::<Vec<_>>();
        let mut running = FuturesUnordered::new();
        let mut last_completed = None;

        loop {
            let mut idx = 0;
            while idx < pending.len() {
                let (system_id, _, n) = &mut pending[idx];
                let (start, end) = self.system_dependencies[*system_id];
                let rest = &self.dependencies[start + *n..end];

                if !rest.is_empty() && rest.first().copied() == last_completed {
                    *n += 1;
                }

                if start + *n >= end {
                    let (id, sys, _) = pending.remove(idx);

                    let f = async move {
                        sys.update().await;
                        SystemID(id as u32)
                    }.boxed();
                    running.push(f);
                    continue;
                }

                idx += 1;
            }

            if running.is_empty() {
                // If `pending` is not empty here, there is a bad dependency.
                break;
            }

            let finished = running.next().await.unwrap();
            last_completed = Some(finished);
        }
    }
}
