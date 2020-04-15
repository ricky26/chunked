use std::fmt::Display;
use std::pin::Pin;
use std::fmt::Debug;
use std::sync::{Arc};
use std::cmp::{Ord};
use std::future::Future;
use std::task::Poll;
use futures::future;
use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::entity::{Component, ComponentType,};
use crate::world::{Snapshot, SnapshotWriter, World};

#[async_trait]
pub trait ChunkSystem {
    async fn update(&mut self, snapshot: &Arc<Snapshot>, writer: &SnapshotWriter);
}

#[async_trait]
pub trait SnapshotSystem {
    async fn update(&mut self, snapshot: &Arc<Snapshot>) -> Arc<Snapshot>;
}

enum SystemRef {
    Snapshot(Box<dyn SnapshotSystem + Send + 'static>),
    Chunk(Box<dyn ChunkSystem + Send + 'static>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SystemToken(pub usize);

pub struct SystemRegistration {
    system: SystemRef,
    before: Vec<SystemToken>,
    after: Vec<SystemToken>,
    read: Vec<ComponentType>,
    write: Vec<ComponentType>,
}

impl SystemRegistration {
    fn new(system: SystemRef) -> SystemRegistration {
        SystemRegistration {
            system,
            before: Vec::new(),
            after: Vec::new(),
            read: Vec::new(),
            write: Vec::new(),
        }
    }

    pub fn new_snapshot(system: impl SnapshotSystem + Send + 'static) -> SystemRegistration {
        let boxed = Box::new(system) as Box<dyn SnapshotSystem + Send + 'static>;
        SystemRegistration::new(SystemRef::Snapshot(boxed))
    }

    pub fn new_chunk(system: impl ChunkSystem + Send + 'static) -> SystemRegistration {
        let boxed = Box::new(system) as Box<dyn ChunkSystem + Send + 'static>;
        SystemRegistration::new(SystemRef::Chunk(boxed))
    }

    pub fn before(mut self, system: SystemToken) -> Self {
        if let Err(insert_idx) = self.before.binary_search(&system) {
            self.before.insert(insert_idx, system);
        }

        self
    }

    pub fn after(mut self, system: SystemToken) -> Self {
        if let Err(insert_idx) = self.after.binary_search(&system) {
            self.after.insert(insert_idx, system);
        }

        self
    }

    pub fn read_component_type(mut self, component_type: ComponentType) -> Self {
        self.read.push(component_type);
        self
    }

    pub fn read<T: Component>(self) -> Self {
        self.read_component_type(ComponentType::for_type::<T>())
    }

    pub fn write_component_type(mut self, component_type: ComponentType) -> Self {
        self.write.push(component_type);
        self
    }

    pub fn write<T: Component>(self) -> Self {
        self.write_component_type(ComponentType::for_type::<T>())
    }
}

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

    pub fn will_write(&self) -> bool {
        match &self.registration.system {
            &SystemRef::Snapshot(_) => true,
            &SystemRef::Chunk(_) => !self.registration.write.is_empty(),
        }
    }

    /// Spawn an execution of this system.
    /// 
    /// This is unsafe as you /must/ wait for the task to complete before using
    /// this system for anything else.
    pub unsafe fn spawn(&mut self, snapshot: &Arc<Snapshot>, writer: &SnapshotWriter) -> JoinHandle<()> {
        let mut sys: &mut SystemRef = std::mem::transmute(&mut self.registration.system);
        let snapshot = snapshot.clone();
        let writer: &'static SnapshotWriter = std::mem::transmute(writer);

        tokio::task::spawn(async move {
            match &mut sys {
                &mut SystemRef::Chunk(sys) => sys.update(&snapshot, writer).await,
                &mut SystemRef::Snapshot(sys) => {
                    let snapshot = sys.update(&snapshot).await;
                    writer.set_snapshot(snapshot);
                },
            }
        })
    }
}

/// A set of systems which are used to modify a world.
pub struct SystemSet {
    next_system_id: usize,
    num_phases: usize,
    systems: Vec<SystemSetSystem>,
    systems_dirty: bool,

    active_systems: Vec<(JoinHandle<()>, bool)>,
}

impl SystemSet {
    pub fn new() -> SystemSet {
        SystemSet {
            next_system_id: 1,
            num_phases: 0,
            systems: Vec::new(),
            systems_dirty: false,
            active_systems: Vec::new(),
        }
    }

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

    pub async fn update(&mut self, world: &Arc<World>) {
        if self.systems_dirty {
            self.update_systems()
        }

        let mut num_woken = 0;
        let mut num_writers: usize = 0;
        let mut snapshot = world.snapshot();

        for phase in 0..self.num_phases {
            let writer = SnapshotWriter::new(snapshot.clone());

            while let Some(sys) = self.systems.get_mut(num_woken).filter(|s| s.phase == phase) {
                num_woken += 1;

                let will_write = sys.will_write();
                if will_write {
                    num_writers += 1;
                }

                self.active_systems.push((unsafe { sys.spawn(&snapshot, &writer) }, will_write));
            }

            while num_writers > 0 {
                let futures = &mut self.active_systems;
                let done_idx = future::poll_fn(|cx| {
                    for (idx, f) in futures.into_iter().enumerate() {
                        match Pin::new(&mut f.0).poll(cx) {
                            Poll::Ready(_) => return Poll::Ready(idx),
                            Poll::Pending => {},
                        }
                    }

                    Poll::Pending
                }).await;

                if futures[done_idx].1 {
                    num_writers -= 1;
                }

                self.active_systems.remove(done_idx);
            }

            snapshot = writer.into_inner();
        }

        while !self.active_systems.is_empty() {
            let futures = &mut self.active_systems;
            let done_idx = future::poll_fn(|cx| {
                for (idx, f) in futures.into_iter().enumerate() {
                    match Pin::new(&mut f.0).poll(cx) {
                        Poll::Ready(_) => return Poll::Ready(idx),
                        Poll::Pending => {},
                    }
                }

                Poll::Pending
            }).await;

            self.active_systems.remove(done_idx);
        }

        world.set_snapshot(snapshot);
    }
}
