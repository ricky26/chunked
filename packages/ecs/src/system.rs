//! Composable ECS systems.

use std::fmt::Display;
use std::fmt::Debug;
use std::sync::{Arc};
use std::cmp::{Ord};
use futures::future::{self, FutureExt};
use async_trait::async_trait;

use crate::entity::{Component, ComponentType,};
use crate::world::{SnapshotWriter, World};

/// An ECS system.
#[async_trait]
pub trait System {
    /// Update the system.
    /// 
    /// Any changes to the world will be represented in `writer`.
    async fn update(&mut self, writer: &SnapshotWriter);
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

struct SystemSetSystem {
    registration: SystemRegistration,
    token: SystemToken,
    phase: usize,
}

impl SystemSetSystem {
    pub fn new(registration: SystemRegistration, token: SystemToken) -> SystemSetSystem {
        SystemSetSystem {
            registration,
            token,
            phase: 0,
        }
    }

    /// Spawn an execution of this system.
    /// 
    /// This is unsafe as you /must/ wait for the task to complete before using
    /// this system for anything else.
    pub async fn update(&mut self, writer: &SnapshotWriter) {
        self.registration.system.update(writer).await
    }
}

/// A set of systems which are used to modify a world.
pub struct SystemSet {
    next_system_id: usize,
    num_phases: usize,
    systems: Vec<SystemSetSystem>,
    systems_dirty: bool,
}

impl SystemSet {
    /// Create a new empty `SystemSet`.
    pub fn new() -> SystemSet {
        SystemSet {
            next_system_id: 1,
            num_phases: 0,
            systems: Vec::new(),
            systems_dirty: false,
        }
    }

    /// Insert a system to the set according to its registration requirements.
    pub fn insert(&mut self, system: SystemRegistration) -> Result<SystemToken, SystemRegistrationError> {
        let token = SystemToken(self.next_system_id);
        self.next_system_id += 1;
        let system = SystemSetSystem::new(system, token);

        let lo = system.registration.after.iter()
            .filter_map(|token| self.systems.binary_search_by_key(token, |r| r.token).ok())
            .max()
            .unwrap_or(0);
        let hi = system.registration.before.iter()
            .filter_map(|token| self.systems.binary_search_by_key(token, |r| r.token).ok())
            .min()
            .unwrap_or(self.systems.len());

        if lo <= hi {
            self.systems.insert(hi, system);
            self.systems_dirty = true;
            Ok(token)
        } else {
            Err(SystemRegistrationError)
        }
    }

    fn update_systems(&mut self) {
        self.systems_dirty = false;

        let mut current_phase = 0;

        for sys in self.systems.iter_mut() {
            sys.phase = current_phase;
            current_phase += 1;
        }
        
        self.num_phases = current_phase + 1;
    }

    /// Run an update for every system.
    pub async fn update(&mut self, world: &Arc<World>) {
        if self.systems_dirty {
            self.update_systems()
        }

        let mut to_run = &mut self.systems[..];
        let mut snapshot = world.snapshot();

        for phase in 0..self.num_phases {
            let writer = SnapshotWriter::new(snapshot);
            let mut active_systems = Vec::with_capacity(to_run.len());

            while to_run.first().map_or(false, |s| s.phase <= phase) {
                let (woken, rest) = to_run.split_first_mut().unwrap();
                to_run = rest;

                active_systems.push(woken.update(&writer).boxed());
            }

            future::join_all(&mut active_systems).await;
            
            drop(active_systems);
            snapshot = writer.into_inner();
        }

        world.set_snapshot(snapshot);
    }
}
