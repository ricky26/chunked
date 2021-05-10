//! A world which can hold entities.

use std::collections::VecDeque;
use std::future::Future;
use std::mem::transmute;
use std::sync::Arc;

use futures::channel::{mpsc, oneshot};
use futures::future;
use futures::future::Either;
use futures::lock::{Mutex, MutexGuard};
use futures::stream::FuturesUnordered;
use futures::StreamExt;

pub use chunk::ChunkGuard;
pub use chunk_set::{ChunkIter, ChunkSetGuard};
pub(crate) use transaction::Transaction;
pub use transaction::TransactionGuard;

use crate::component::ComponentTypeID;
use crate::snapshot::Snapshot;
use crate::universe::Universe;
use crate::world::transaction::locks_include_archetype;

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
        matches!(self, Lock::Read(_))
    }

    /// Returns true if this is a write lock.
    pub fn is_write(&self) -> bool {
        matches!(self, Lock::Write(_))
    }

    /// Returns true if this is an exclusion.
    pub fn is_exclude(&self) -> bool {
        matches!(self, Lock::Exclude(_))
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

type BoxedTransaction = Box<dyn for<'a> FnOnce(TransactionGuard<'a>) + Send + 'static>;
type TransactionCommand = (Vec<Lock>, BoxedTransaction, oneshot::Sender<()>);

/// A World manages an evolving series of `Snapshot`s.
///
/// Worlds introduce the ability to do parallel work on a snapshot at the cost
/// of requiring futures in most cases.
///
/// You can create an empty world with an empty snapshot with `World::new()`.
///
/// Cloning the `World` produces another view onto the same world.
pub struct World {
    universe: Arc<Universe>,
    snapshot: Arc<Mutex<Arc<Snapshot>>>,
    exit_rx: oneshot::Receiver<()>,
    transaction_tx: mpsc::UnboundedSender<TransactionCommand>,
}

impl World {
    /// Create a new world.
    pub fn new(universe: Arc<Universe>) -> World {
        Self::with_snapshot(Arc::new(Snapshot::empty(universe)))
    }

    /// Create a new world with a given initial snapshot.
    pub fn with_snapshot(snapshot: Arc<Snapshot>) -> World {
        let universe = snapshot.universe().clone();
        let snapshot = Arc::new(Mutex::new(snapshot));
        let (transaction_tx, transaction_rx) = mpsc::unbounded();
        let (exit_tx, exit_rx) = oneshot::channel();

        let transactions = WorldTransactions::new(snapshot.clone());
        tokio::spawn(async move {
            let _ = exit_tx;
            transactions.handle_commands(transaction_rx).await;
        });

        World {
            universe,
            snapshot,
            exit_rx,
            transaction_tx,
        }
    }

    /// Return the universe this world exists inside.
    pub fn universe(&self) -> &Arc<Universe> { &self.universe }

    /// Drop this world and return the contained snapshot.
    ///
    /// If this is not the last view onto the same world, an error will
    /// be returned with its original value.
    pub async fn into_snapshot(self) -> Arc<Snapshot> {
        drop(self.transaction_tx);
        self.exit_rx.await.ok();
        Arc::try_unwrap(self.snapshot).unwrap().into_inner()
    }

    /// Create a snapshot of the current world state.
    pub async fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.lock().await.clone()
    }

    /// Replace the snapshot of the world, returning the old snapshot.
    pub async fn replace_snapshot(&self, snapshot: Arc<Snapshot>) -> Arc<Snapshot> {
        assert!(Arc::ptr_eq(snapshot.universe(), &self.universe),
                "snapshot is not of this universe");
        std::mem::replace(&mut *self.snapshot.lock().await, snapshot)
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
        let mut snapshot = self.snapshot.lock().await;
        f(&mut *snapshot);
    }

    /// Run a non-structural transaction.
    pub async fn transaction<F>(&self, locks: impl Into<Vec<Lock>>, f: F)
        where for<'a> F: FnOnce(TransactionGuard<'a>) + Send + 'static
    {
        let f = Box::new(f) as Box<dyn for<'a> FnOnce(TransactionGuard<'a>) + Send>;
        let (signal_tx, signal_rx) = oneshot::channel();
        self.transaction_tx.unbounded_send((locks.into(), f, signal_tx)).unwrap();
        signal_rx.await.ok();
    }
}

struct WorldTransactions {
    lock: Option<MutexGuard<'static, Arc<Snapshot>>>,
    snapshot: Arc<Mutex<Arc<Snapshot>>>,

    #[allow(clippy::vec_box)]
    active_transactions: Vec<Box<Transaction>>,
    pending_transactions: VecDeque<TransactionCommand>,

    next_transaction: u32,
}

impl WorldTransactions {
    /// Create a new `World` parallel transaction manager.
    pub fn new(snapshot: Arc<Mutex<Arc<Snapshot>>>) -> WorldTransactions {
        WorldTransactions {
            lock: None,
            snapshot,

            active_transactions: Vec::new(),
            pending_transactions: VecDeque::new(),

            next_transaction: 0,
        }
    }

    /// Add the transaction to the transaction list and return the future to execute it.
    fn start_transaction(&mut self, cmd: TransactionCommand) -> impl Future<Output=u32> {
        let (locks, f, tx) = cmd;

        let id = self.next_transaction;
        self.next_transaction += 1;

        // We can transmute this to a &'static mut because:
        // - We are going to protect the lifetime (we release the transaction when it's done)
        // - Only chunk edits are done and we manually make sure they don't alias
        let mut_snap = Arc::make_mut(self.lock.as_mut().unwrap());
        let snapshot: &'static mut Snapshot = unsafe { transmute(mut_snap) };

        let archetypes = snapshot.chunk_sets().iter()
            .map(|chunk_set| chunk_set.archetype())
            .filter(|a| transaction::locks_include_archetype(a, &locks))
            .cloned()
            .collect();
        let transaction = Box::new(
            Transaction::new(id, archetypes, locks));
        let transaction_ref: &Transaction = unsafe { transmute(&*transaction as &Transaction) };
        self.active_transactions.push(transaction);
        let (done_tx, done_rx) = oneshot::channel::<()>();

        let guard = TransactionGuard::new(transaction_ref, snapshot);

        rayon::spawn(move || {
            let _ = done_tx;
            let _ = tx;
            (f)(guard);
        });

        async move {
            done_rx.await.ok();
            id
        }
    }

    /// Check whether a new transaction could coincide with an existing one.
    fn locks_compatible(&self, existing: &[Lock], new: &[Lock]) -> bool {
        let snap = self.lock.as_ref().unwrap();

        // First check whether they would be compatible in the same chunks:
        let mut can_lock = true;
        for lock in new {
            match lock {
                Lock::Read(type_id) => {
                    let ok = existing.iter()
                        .all(|l| !l.is_write() || l.type_id() != *type_id);
                    if !ok {
                        can_lock = false;
                        break;
                    }
                }
                Lock::Write(type_id) => {
                    let ok = existing.iter()
                        .all(|l| l.is_exclude() || l.type_id() != *type_id);
                    if !ok {
                        can_lock = false;
                        break;
                    }
                }
                Lock::Exclude(_) => {}
            }
        }

        if can_lock {
            return true;
        }

        // If they do collide, check whether they are distinct chunk sets.
        for chunk_set in snap.chunk_sets().iter() {
            if locks_include_archetype(chunk_set.archetype(), existing)
                && locks_include_archetype(chunk_set.archetype(), new) {
                return false;
            }
        }

        true
    }

    /// Checks if it is safe to start a new transaction with the given locks.
    fn can_start(&self, locks: &[Lock]) -> bool {
        self.active_transactions.iter()
            .all(|t| self.locks_compatible(t.locks(), locks))
    }

    /// Run this manager until the `World` is dropped.
    pub async fn handle_commands(
        mut self,
        mut commands_rx: mpsc::UnboundedReceiver<TransactionCommand>,
    ) {
        let mut exit = false;
        let mut active_futures = FuturesUnordered::new();

        loop {
            let task_done = if active_futures.is_empty() {
                Either::Left(commands_rx.next().await)
            } else {
                match future::select(commands_rx.next(), active_futures.next()).await {
                    Either::Left((x, _)) => Either::Left(x),
                    Either::Right((x, _)) => Either::Right(x),
                }
            };

            match task_done {
                Either::Left(None) => {
                    exit = true;

                    if active_futures.is_empty() {
                        return;
                    }
                }
                Either::Left(Some((locks, f, tx))) => {
                    // Start immediately if we can.
                    if self.can_start(&locks) {
                        if self.active_transactions.is_empty() && self.lock.is_none() {
                            let lock = unsafe { std::mem::transmute(self.snapshot.lock().await) };
                            self.lock = Some(lock);
                        }

                        active_futures.push(self.start_transaction((locks, f, tx)));
                    } else {
                        self.pending_transactions.push_back((locks, f, tx));
                    }
                }
                Either::Right(Some(done)) => {
                    self.active_transactions.retain(|t| t.id() != done);

                    // Start any pending transactions which are now possible.
                    let mut i = 0;
                    while i < self.pending_transactions.len() {
                        let (locks, _, _) = &self.pending_transactions[i];
                        if self.can_start(locks) {
                            let cmd = self.pending_transactions.remove(i).unwrap();
                            let f = self.start_transaction(cmd);
                            active_futures.push(f);
                        } else {
                            i += 1;
                        }
                    }

                    if active_futures.is_empty() {
                        self.lock.take();

                        if exit {
                            return;
                        }
                    }
                }
                _ => panic!("unexpected future wakeup in WorldTransactions")
            }
        }
    }
}