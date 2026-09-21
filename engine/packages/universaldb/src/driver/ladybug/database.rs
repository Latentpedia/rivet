use std::{
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicI32, Ordering},
	},
};

use anyhow::{Context, Result, bail};
use lbug::{Database as LbugDatabase, SystemConfig};

use crate::{
	RetryableTransaction, Transaction,
	driver::{BoxFut, DatabaseDriver, Erased},
	error::DatabaseError,
	transaction::TXN_TIMEOUT,
	utils::calculate_tx_retry_backoff,
};

use super::transaction::{LadybugTransaction, LadybugTransactionDriver};

/// Configuration for opening a ladybug graph database.
///
/// The fields mirror the options LadybugDB surfaces through [`SystemConfig`].
#[derive(Clone, Debug)]
pub struct LadybugConfig {
	/// When true, the store automatically checkpoints the write-ahead log into the data
	/// files once the log exceeds [`Self::checkpoint_threshold`] bytes.
	pub auto_checkpoint: bool,
	/// Write-ahead-log size (bytes) that triggers an automatic checkpoint.
	pub checkpoint_threshold: i64,
	/// Verifies WAL checksums on replay to detect corruption.
	pub enable_checksums: bool,
	/// Allows multiple concurrent write transactions.
	pub enable_multi_writes: bool,
	/// Opens the store read-only.
	pub read_only: bool,
}

impl Default for LadybugConfig {
	fn default() -> Self {
		LadybugConfig {
			auto_checkpoint: true,
			checkpoint_threshold: -1,
			enable_checksums: true,
			enable_multi_writes: false,
			read_only: false,
		}
	}
}

impl LadybugConfig {
	fn to_system_config(&self) -> SystemConfig {
		SystemConfig::default()
			.auto_checkpoint(self.auto_checkpoint)
			.checkpoint_threshold(self.checkpoint_threshold)
			.enable_checksums(self.enable_checksums)
			.enable_multi_writes(self.enable_multi_writes)
			.read_only(self.read_only)
	}
}

pub(crate) struct SharedInternal {
	pub(crate) db: LbugDatabase,
	// Serializes write transactions (BEGIN .. ops .. COMMIT) so a whole transaction runs
	// atomically relative to other writers. `parking_lot` is safe here because the guard is only
	// ever held across synchronous FFI calls and never across an `.await`.
	pub(crate) write_lock: parking_lot::Mutex<()>,
}

pub struct LadybugDatabaseDriver {
	shared: Arc<SharedInternal>,
	max_retries: AtomicI32,
}

impl LadybugDatabaseDriver {
	pub async fn new(path: PathBuf, config: LadybugConfig) -> Result<Self> {
		tracing::info!(db_path=%path.display(), "starting ladybug graph driver");

		// LadybugDB requires the parent directory to exist; the path itself is the database
		// prefix (data file), not a directory. The parent is only created to mirror the
		// convenience of the rocksdb driver.
		if let Some(parent) = path.parent() {
			std::fs::create_dir_all(parent)
				.context("failed to create ladybug database parent directory")?;
		}

		let sys = config.to_system_config();
		let db = LbugDatabase::new(&path, sys).with_context(|| {
			format!(
				"failed to open ladybug graph database at {}",
				path.display()
			)
		})?;

		Ok(LadybugDatabaseDriver {
			shared: Arc::new(SharedInternal {
				db,
				write_lock: parking_lot::Mutex::new(()),
			}),
			max_retries: AtomicI32::new(100),
		})
	}

	/// Opens an in-memory graph database. Data is lost when the driver is dropped.
	pub fn in_memory() -> Result<Self> {
		let config = LadybugConfig::default();
		let db = LbugDatabase::in_memory(config.to_system_config())
			.context("failed to open in-memory ladybug graph database")?;

		Ok(LadybugDatabaseDriver {
			shared: Arc::new(SharedInternal {
				db,
				write_lock: parking_lot::Mutex::new(()),
			}),
			max_retries: AtomicI32::new(100),
		})
	}

	/// Returns a new graph transaction bound to this database.
	///
	/// This is the supported surface for interacting with the graph, in contrast with the
	/// key/value [`DatabaseDriver`] surface which is present only for lifecycle parity.
	pub fn graph_txn(&self) -> LadybugTransaction {
		LadybugTransaction::new(self.shared.clone())
	}

	/// Runs the given closure inside a graph transaction and commits it.
	///
	/// The closure receives a [`LadybugTransaction`] that buffers writes and executes reads
	/// against the live graph. When the closure returns `Ok`, the buffered writes are applied
	/// atomically inside `BEGIN TRANSACTION .. COMMIT`; on retryable failures the whole
	/// transaction is retried with a bounded backoff.
	pub async fn run_graph<'a, F, R>(&'a self, closure: F) -> Result<R>
	where
		F: Fn(LadybugTransaction) -> BoxFut<'a, Result<R>> + Send + Sync + 'a,
		R: Send + 'a,
	{
		let max_retries = self.max_retries.load(Ordering::SeqCst);

		for attempt in 0..max_retries {
			let txn = self.graph_txn();
			let result = match tokio::time::timeout(TXN_TIMEOUT, closure(txn.clone())).await {
				Ok(Ok(value)) => match txn.commit().await {
					Ok(()) => return Ok(value),
					Err(err) => err,
				},
				Ok(Err(err)) => {
					txn.abort();
					err
				}
				Err(_) => anyhow::Error::from(DatabaseError::TransactionTooOld),
			};

			let chain = result.downcast_ref::<DatabaseError>();
			if let Some(db_error) = chain {
				if db_error.is_retryable() {
					let backoff_ms = calculate_tx_retry_backoff(attempt as usize);
					tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
					continue;
				}
			}

			return Err(result);
		}

		Err(DatabaseError::MaxRetriesReached(anyhow::anyhow!(
			"ladybug graph transaction exhausted {max_retries} retries"
		))
		.into())
	}

	/// Flushes the write-ahead log into the persistent data files.
	///
	/// Unlike the rocksdb driver, which copies a point-in-time snapshot to `path`, LadybugDB
	/// checkpoints its WAL in place, so this ignores `_path` and only forces the durability of
	/// already-committed transactions in the store's home directory.
	pub fn force_checkpoint(&self) -> Result<()> {
		let _guard = self.shared.write_lock.lock();
		let conn = lbug::Connection::new(&self.shared.db)
			.context("failed to create ladybug connection for checkpoint")?;
		conn.query("CHECKPOINT")
			.context("failed to run ladybug CHECKPOINT")?;
		Ok(())
	}
}

impl DatabaseDriver for LadybugDatabaseDriver {
	fn create_txn(&self) -> Result<Transaction> {
		// Present only for lifecycle parity. Key/value operations on the returned transaction
		// fail by default; use `graph_txn` / `run_graph` to interact with the graph.
		Ok(Transaction::new(Arc::new(LadybugTransactionDriver)))
	}

	fn run<'a>(
		&'a self,
		_closure: Box<
			dyn Fn(RetryableTransaction) -> BoxFut<'a, Result<Erased>> + Send + Sync + 'a,
		>,
	) -> BoxFut<'a, Result<Erased>> {
		Box::pin(async move {
			bail!(
				"the ladybug (graph) driver does not support the key/value `run` path; \
				 use `LadybugDatabaseDriver::run_graph` for graph transactions"
			)
		})
	}

	fn txn_retry_limit(&self, limit: i32) -> Result<()> {
		self.max_retries.store(limit, Ordering::SeqCst);
		Ok(())
	}

	fn checkpoint(&self, _path: &Path) -> Result<()> {
		self.force_checkpoint()
	}
}
