//! Composable ECS systems.

use std::fmt::Display;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc};
use std::future::Future;
use std::task::{Poll, Context};
use std::cmp::{Ord};
use futures::future;
use async_trait::async_trait;

use crate::entity::{Component, ComponentType};
use crate::snapshot::SnapshotWriter;
use crate::world::World;
use crate::reusable::{Reusable, ReusableAlloc};

/// An ECS system.
#[async_trait]
pub trait System {
    /// Update the system.
    /// 
    /// Any changes to the world will be represented in `writer`.
    async fn update(&mut self, writer: SnapshotWriter);
}

/// A token which represents a system in a `SystemSet`.
/// 
/// These tokens are not unique between `SystemSet`s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SystemToken(pub usize);

type BoxedSystem = Box<dyn System + Send + 'static>;

/// A registration used for building `SystemSet`s.
pub struct SystemRegistration {
    system: BoxedSystem,
    before: Vec<SystemToken>,
    after: Vec<SystemToken>,
    read: Vec<ComponentType>,
    write: Vec<ComponentType>,
    barrier: bool,
}

impl SystemRegistration {
    /// Create a new registration from the boxed system.
    fn new(system: BoxedSystem) -> SystemRegistration {
        SystemRegistration {
            system,
            before: Vec::new(),
            after: Vec::new(),
            read: Vec::new(),
            write: Vec::new(),
            barrier: false,
        }
    }

    /// Create a new registration from any object implementing `System`.
    pub fn from_system(system: impl System + Send + 'static) -> SystemRegistration {
        let boxed = Box::new(system) as BoxedSystem;
        SystemRegistration::new(boxed)
    }

    /// Require that this system is updated before the system represented
    /// by the given token.
    pub fn before(mut self, system: SystemToken) -> Self {
        if let Err(insert_idx) = self.before.binary_search(&system) {
            self.before.insert(insert_idx, system);
        }

        self
    }

    /// Require that this system is updated after the system represented
    /// by the given token.
    pub fn after(mut self, system: SystemToken) -> Self {
        if let Err(insert_idx) = self.after.binary_search(&system) {
            self.after.insert(insert_idx, system);
        }

        self
    }

    /// Declare that this system reads the given component types.
    pub fn read_component_type(mut self, component_type: ComponentType) -> Self {
        self.read.push(component_type);
        self
    }

    /// Declare that this system reads the given component types.
    pub fn read<T: Component>(self) -> Self {
        self.read_component_type(ComponentType::for_type::<T>())
    }

    /// Declare that this system writes the given component types.
    pub fn write_component_type(mut self, component_type: ComponentType) -> Self {
        self.write.push(component_type);
        self
    }

    /// Declare that this system writes the given component types.
    pub fn write<T: Component>(self) -> Self {
        self.write_component_type(ComponentType::for_type::<T>())
    }

    /// Require that this system has exclusive access to the world during its
    /// update.
    pub fn barrier(mut self) -> Self {
        self.barrier = true;
        self
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

impl std::error::Error for SystemRegistrationError {
}

type BoxedSystemUpdate = Reusable<dyn Future<Output=BoxedSystem>>;

enum SystemState {
    Invalid,
    Idle(BoxedSystem, ReusableAlloc),
    Running(BoxedSystemUpdate),
}

struct SystemSetSystem {
    state: SystemState,
    barrier: bool,
    phase: usize,
    num_blockers: usize,
    dep_count: usize,
    inv_deps: Vec<usize>,
}

impl SystemSetSystem {
    pub fn new(system: BoxedSystem) -> SystemSetSystem {
        let state = SystemState::Idle(system, ReusableAlloc::empty());

        SystemSetSystem {
            state,
            barrier: false,
            phase: 0,
            num_blockers: 0,
            dep_count: 0,
            inv_deps: Vec::new(),
        }
    }

    pub fn ready(&self) -> bool {
        self.num_blockers == 0
    }

    pub fn reset(&mut self) {
        self.num_blockers = self.dep_count;
    }

    pub fn start(&mut self, writer: SnapshotWriter) {
        let state = std::mem::replace(&mut self.state, SystemState::Invalid);
        self.state = match state {
            SystemState::Idle(mut system, alloc) => {
                let f = async {
                    system.update(writer).await;
                    system
                };

                // Some unsafe magic to finangle our custom pointer type
                // into a trait object.
                let alloc = Reusable::from_alloc(alloc, f);
                let ptr = alloc.as_ptr();
                let layout = alloc.layout();
                std::mem::forget(alloc);

                let boxed = unsafe {
                    let ptr = ptr as *mut dyn Future<Output=BoxedSystem>;
                    let ptr = std::ptr::NonNull::new_unchecked(ptr);

                    Reusable::from_raw(ptr, layout)
                };
                
                SystemState::Running(boxed)
            },
            x => x,
        };
    }

    pub fn poll(&mut self, cx: &mut Context) -> Poll<()> {
        let state = std::mem::replace(&mut self.state, SystemState::Invalid);
        let (new_state, ret) = match state {
            SystemState::Running(mut f) => {
                match Pin::new(&mut f).poll(cx) {
                    Poll::Ready(ptr) =>
                        (SystemState::Idle(ptr, f.free()), Poll::Ready(())),
                    Poll::Pending =>
                        (SystemState::Running(f), Poll::Pending),
                }
            },
            _ => (state, Poll::Pending),
        };
        self.state = new_state;
        ret
    }
}

/// A set of systems which are used to modify a world.
pub struct SystemSet {
    next_system_id: usize,
    num_phases: usize,

    systems: Vec<SystemSetSystem>,
    sorted_systems: Vec<usize>,
    remapping: Vec<usize>,
    systems_dirty: bool,
}

impl SystemSet {
    /// Create a new empty `SystemSet`.
    pub fn new() -> SystemSet {
        SystemSet {
            next_system_id: 0,
            num_phases: 0,

            systems: Vec::new(),
            sorted_systems: Vec::new(),
            remapping: Vec::new(),
            systems_dirty: false,
        }
    }

    /// Insert a system to the set according to its registration requirements.
    pub fn insert(&mut self, registration: SystemRegistration) -> Result<SystemToken, SystemRegistrationError> {
        let token = SystemToken(self.next_system_id);
        self.next_system_id += 1;
        let mut system = SystemSetSystem::new(registration.system);
        system.barrier = registration.barrier;

        let after_indices = match registration.after.iter()
            .map(|token| self.sorted_systems.iter().cloned().filter(|idx| *idx == token.0).next())
            .collect::<Option<Vec<_>>>() {
            Some(x) => x,
            None => Err(SystemRegistrationError)?,
        };
        let before_indices = match registration.before.iter()
            .map(|token| self.sorted_systems.iter().cloned().filter(|idx| *idx == token.0).next())
            .collect::<Option<Vec<_>>>() {
            Some(x) => x,
            None => Err(SystemRegistrationError)?,
        };

        let lo = after_indices.iter()
            .cloned()
            .max()
            .unwrap_or(0);
        let hi = before_indices.iter()
            .cloned()
            .min()
            .unwrap_or(self.systems.len());
        
        if lo > hi {
            return Err(SystemRegistrationError)
        }

        let index = self.systems.len();

        for token in registration.after.iter() {
            let sys = &mut self.systems[token.0];
            if sys.inv_deps.contains(&index) {
                continue;
            }

            system.dep_count += 1;
            sys.inv_deps.push(index);
        }

        for token in registration.before.iter() {
            if system.inv_deps.contains(&token.0) {
                continue;
            }

            let sys = &mut self.systems[token.0];
            sys.dep_count += 1;
            system.inv_deps.push(token.0);
        }

        self.systems.push(system);
        self.sorted_systems.insert(hi, index);
        self.systems_dirty = true;
        Ok(token)
    }

    fn update_systems(&mut self) {
        self.systems_dirty = false;

        let mut current_phase = 0;

        for sys in self.systems.iter_mut() {
            if sys.barrier {
                sys.phase = current_phase + 1;
                current_phase = sys.phase + 1;
            } else {
                sys.phase = current_phase;
            }
        }
        
        self.num_phases = current_phase + 1;
    }

    /// Run an update for every system.
    pub async fn update(&mut self, world: &Arc<World>) {
        if self.systems_dirty {
            self.update_systems()
        }

        let systems = &mut self.systems;
        let remapping = &mut self.remapping;
        let mut done = 0;
        let mut snapshot = world.snapshot();

        remapping.clear();
        remapping.extend(0..systems.len());

        for sys in systems.iter_mut() {
            sys.reset();
        }

        for phase in 0..self.num_phases {
            let end = done + remapping.iter()
                .map(|idx| &systems[*idx])
                .take_while(|sys| sys.phase <= phase)
                .count();

            let (writer, reader) = SnapshotWriter::new(snapshot);
            let mut running = 0;
            let mut ready = 0;

            // Kick off all initially-ready systems.
            for idx in done..end {
                if systems[remapping[idx]].ready() {
                    let write_idx = ready;
                    ready += 1;

                    if write_idx != idx {
                        let (a, b) = remapping.split_at_mut(idx);
                        std::mem::swap(&mut a[write_idx], &mut b[0]);
                    }
                }
            }

            while done < end {
                // Start new ready systems.
                for idx in running..ready {
                    systems[remapping[idx]].start(writer.clone());
                }
                running = ready;

                // Wait for a system to be done.
                let done_idx = future::poll_fn(|cx| {
                    for idx in done..ready {
                        let sys = &mut systems[remapping[idx]];
                        if let Poll::Ready(_) = sys.poll(cx) {
                            return Poll::Ready(idx);
                        }
                    }

                    Poll::Pending
                }).await;

                if done_idx != done {
                    let (a, b) = remapping.split_at_mut(done_idx);
                    std::mem::swap(&mut a[done], &mut b[0]);
                }

                let num_to_wake = systems[remapping[done]].inv_deps.len();

                // Prepare all now ready systems.
                for idx_idx in 0..num_to_wake {
                    let idx = systems[remapping[done]].inv_deps[idx_idx];
                    let sys = &mut systems[idx];

                    sys.num_blockers -= 1;

                    if sys.ready() {
                        if idx != ready {
                            let (a, b) = remapping.split_at_mut(idx);
                            std::mem::swap(&mut a[ready], &mut b[0]);
                        }

                        ready += 1;
                    }
                }

                done += 1;
            }

            drop(writer);
            snapshot = reader.await.unwrap();
        }

        world.set_snapshot(snapshot);
    }
}
