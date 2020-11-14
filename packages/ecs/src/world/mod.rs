//! A world which can hold entities.

use std::sync::Arc;

use futures::channel::{mpsc, oneshot};
use futures::executor::LocalPool;
use futures::StreamExt;

pub use chunk::ChunkGuard;
pub use chunk_set::{ChunkIter, ChunkSetGuard};
pub use transaction::TransactionGuard;
pub(crate) use transaction::Transaction;

use crate::component::ComponentTypeID;
use crate::snapshot::Snapshot;
use crate::universe::Universe;

mod chunk;
mod chunk_set;
mod transaction;

/// `Lock`s are used to define a subset of the snapshot to transact.
///
/// Multiple transactions with conflicting locks will block one another.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Lock {
    Read(ComponentTypeID),
    Write(ComponentTypeID),
    Exclude(ComponentTypeID),
}

impl Lock {
    /// Returns true if this is a read lock.
    pub fn is_read(&self) -> bool {
        match self {
            Lock::Read(_) => true,
            _ => false,
        }
    }

    /// Returns true if this is a write lock.
    pub fn is_write(&self) -> bool {
        match self {
            Lock::Write(_) => true,
            _ => false,
        }
    }

    /// Returns true if this is an exclusion.
    pub fn is_exclude(&self) -> bool {
        match self {
            Lock::Exclude(_) => true,
            _ => false,
        }
    }

    /// Return the component type for this lock.
    pub fn type_id(&self) -> ComponentTypeID {
        match self {
            Lock::Read(x) => *x,
            Lock::Write(x) => *x,
            Lock::Exclude(x) => *x,
        }
    }
}

type BoxedExclusiveTransaction = Box<dyn FnOnce(&mut Arc<Snapshot>) + Send + 'static>;
type BoxedTransaction = Box<dyn for<'a> FnOnce(TransactionGuard<'a>) + Send + 'static>;

enum WorldCommand {
    Transaction(Vec<Lock>, BoxedTransaction, oneshot::Sender<()>),
    ExclusiveTransaction(BoxedExclusiveTransaction, oneshot::Sender<()>),
    Snapshot(oneshot::Sender<Arc<Snapshot>>),
    Replace(Arc<Snapshot>, oneshot::Sender<Arc<Snapshot>>),
    Exit(oneshot::Sender<Arc<Snapshot>>),
}

/// A World manages an evolving series of `Snapshot`s.
///
/// Worlds introduce the ability to do parallel work on a snapshot at the cost
/// of requiring futures in most cases.
///
/// You can create an empty world with an empty snapshot with `World::new()`.
pub struct World {
    universe: Arc<Universe>,
    command_tx: mpsc::UnboundedSender<WorldCommand>,
}

impl World {
    /// Create a new world.
    pub fn new(universe: Arc<Universe>) -> World {
        Self::with_snapshot(Arc::new(Snapshot::empty(universe)))
    }

    /// Create a new world with a given initial snapshot.
    pub fn with_snapshot(snapshot: Arc<Snapshot>) -> World {
        let universe = snapshot.universe().clone();
        let (command_tx, command_rx) = mpsc::unbounded();

        std::thread::spawn(move ||
            World::run(snapshot, command_rx));

        World {
            universe,
            command_tx,
        }
    }

    /// Return the universe this world exists inside.
    pub fn universe(&self) -> &Arc<Universe> { &self.universe }

    /// Drop this world and return the contained snapshot.
    pub async fn into_snapshot(self) -> Arc<Snapshot> {
        let (snap_tx, snap_rx) = oneshot::channel();
        self.command_tx.unbounded_send(WorldCommand::Exit(snap_tx)).unwrap();
        snap_rx.await.unwrap()
    }

    /// Create a snapshot of the current world state.
    pub async fn snapshot(&self) -> Arc<Snapshot> {
        let (snap_tx, snap_rx) = oneshot::channel();
        self.command_tx.unbounded_send(WorldCommand::Snapshot(snap_tx)).unwrap();
        snap_rx.await.unwrap()
    }

    /// Replace the snapshot of the world, returning the old snapshot.
    pub async fn replace_snapshot(&self, snapshot: Arc<Snapshot>) -> Arc<Snapshot> {
        assert!(Arc::ptr_eq(snapshot.universe(), &self.universe),
                "snapshot is not of this universe");

        let (snap_tx, snap_rx) = oneshot::channel();
        self.command_tx.unbounded_send(WorldCommand::Replace(snapshot, snap_tx)).unwrap();
        snap_rx.await.unwrap()
    }

    /// Take the current snapshot of the world and clear it.
    /// 
    /// Generally this is designed to be used as a performance optimisation:
    /// if nobody else has a reference to the Snapshot, it can be modified
    /// freely.
    pub async fn take_snapshot(&self) -> Arc<Snapshot> {
        self.replace_snapshot(Arc::new(Snapshot::empty(self.universe.clone()))).await
    }

    /// Set the current state of the world.
    pub async fn set_snapshot(&self, snapshot: Arc<Snapshot>) {
        self.replace_snapshot(snapshot).await;
    }

    /// Clear all entities from the world.
    pub async fn clear(&self) {
        self.take_snapshot().await;
    }

    /// Modify the snapshot.
    ///
    /// This takes the write lock for the snapshot for the entirety of the call
    /// to `f()` and should be used sparingly.
    ///
    /// A good use is for applying command buffers, since it can avoid extra
    /// memory allocation.
    pub async fn exclusive_transaction<F>(&self, f: F)
        where F: FnOnce(&mut Arc<Snapshot>) + Send
    {
        // We know that we're going to block until this function has been dropped.
        let f = Box::new(f) as Box<dyn FnOnce(&mut Arc<Snapshot>) + Send>;
        let f = unsafe { std::mem::transmute(f) };

        let (signal_tx, signal_rx) = oneshot::channel();
        self.command_tx.unbounded_send(
            WorldCommand::ExclusiveTransaction(f, signal_tx))
            .unwrap();

        signal_rx.await.unwrap();
    }

    /// Run a non-structural transaction.
    pub async fn transaction<F>(&self, locks: impl Into<Vec<Lock>>, f: F)
        where for<'a> F: FnOnce(TransactionGuard<'a>) + Send
    {
        // We know that we're going to block until this function has been dropped.
        let f = Box::new(f) as Box<dyn for<'a> FnOnce(TransactionGuard<'a>) + Send>;
        let f = unsafe { std::mem::transmute(f) };

        let (signal_tx, signal_rx) = oneshot::channel();
        self.command_tx.unbounded_send(
            WorldCommand::Transaction(locks.into(), f, signal_tx))
            .unwrap();
        signal_rx.await.unwrap();
    }

    /// This function is created as a thread to process transactions so that they don't block
    /// the thread pool.
    fn run(mut snapshot: Arc<Snapshot>, mut commands_rx: mpsc::UnboundedReceiver<WorldCommand>) {
        let mut pool = LocalPool::new();

        loop {
            let command = match pool.run_until(commands_rx.next()) {
                Some(x) => x,
                None => {
                    pool.run();
                    return;
                }
            };

            match command {
                WorldCommand::Transaction(locks, f, tx) => {
                    let archetypes = (0..snapshot.chunk_sets().len())
                        .map(|idx| snapshot.universe().archetype_by_id(idx).unwrap())
                        .filter(|a| transaction::locks_include_archetype(a, &locks))
                        .collect();

                    let transaction = Transaction::new(snapshot.clone(), archetypes, locks);
                    let guard = TransactionGuard::new(&transaction);
                    (f)(guard);
                    tx.send(()).ok();
                }
                WorldCommand::ExclusiveTransaction(f, tx) => {
                    (f)(&mut snapshot);
                    tx.send(()).ok();
                }
                WorldCommand::Snapshot(tx) => {
                    tx.send(snapshot.clone()).unwrap();
                }
                WorldCommand::Replace(new_snapshot, tx) => {
                    tx.send(std::mem::replace(&mut snapshot, new_snapshot)).unwrap();
                }
                WorldCommand::Exit(tx) => {
                    tx.send(snapshot).unwrap();
                    pool.run();
                    return;
                }
            }
        }
    }
}
