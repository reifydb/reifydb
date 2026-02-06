// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use pending::{Pending, PendingWrites};
use reifydb_catalog::catalog::Catalog;
use reifydb_core::common::CommitVersion;
use reifydb_transaction::{
	interceptor::{
		WithInterceptors,
		chain::InterceptorChain as Chain,
		interceptors::Interceptors,
		namespace_def::{
			NamespaceDefPostCreateInterceptor, NamespaceDefPostUpdateInterceptor,
			NamespaceDefPreDeleteInterceptor, NamespaceDefPreUpdateInterceptor,
		},
		ringbuffer::{
			RingBufferPostDeleteInterceptor, RingBufferPostInsertInterceptor,
			RingBufferPostUpdateInterceptor, RingBufferPreDeleteInterceptor,
			RingBufferPreInsertInterceptor, RingBufferPreUpdateInterceptor,
		},
		ringbuffer_def::{
			RingBufferDefPostCreateInterceptor, RingBufferDefPostUpdateInterceptor,
			RingBufferDefPreDeleteInterceptor, RingBufferDefPreUpdateInterceptor,
		},
		table::{
			TablePostDeleteInterceptor, TablePostInsertInterceptor, TablePostUpdateInterceptor,
			TablePreDeleteInterceptor, TablePreInsertInterceptor, TablePreUpdateInterceptor,
		},
		table_def::{
			TableDefPostCreateInterceptor, TableDefPostUpdateInterceptor, TableDefPreDeleteInterceptor,
			TableDefPreUpdateInterceptor,
		},
		transaction::{PostCommitInterceptor, PreCommitInterceptor},
		view::{
			ViewPostDeleteInterceptor, ViewPostInsertInterceptor, ViewPostUpdateInterceptor,
			ViewPreDeleteInterceptor, ViewPreInsertInterceptor, ViewPreUpdateInterceptor,
		},
		view_def::{
			ViewDefPostCreateInterceptor, ViewDefPostUpdateInterceptor, ViewDefPreDeleteInterceptor,
			ViewDefPreUpdateInterceptor,
		},
	},
	multi::transaction::read::MultiReadTransaction,
	transaction::admin::AdminTransaction,
};
use tracing::instrument;

pub mod pending;
pub mod range;
pub mod read;
pub mod state;
pub mod write;

/// A transaction wrapper for flow processing with dual-version read semantics.
///
/// # Architecture
///
/// FlowTransaction provides **dual-version reads** critical for stateful flow processing:
/// 1. **Source data** - Read at CDC event version (snapshot isolation)
/// 2. **Flow state** - Read at latest version (state continuity across CDC events)
/// 3. **Isolated writes** - Local PendingWrites buffer returned to caller
///
/// This dual-version approach allows stateful operators (joins, aggregates, distinct) to:
/// - Process source data at a consistent snapshot (the CDC event version)
/// - Access their own state at the latest version to maintain continuity
///
/// # Dual-Version Read Routing
///
/// Reads are automatically routed to the correct query transaction based on key type:
///
/// ```text
/// ┌─────────────────┐
/// │  FlowTransaction│
/// └────────┬────────┘
///          │
///          ├──► primitive_query (at CDC version)
///          │    - Source tables
///          │    - Source views
///          │    - Regular data
///          │
///          └──► state_query (at latest version)
///               - FlowNodeState
///               - FlowNodeInternalState
///               - Stateful operator state
/// ```
///
/// # Why Dual Versions Matter
///
/// ## Example: Join Operator
/// ```ignore
/// // CDC event arrives at version 100
/// let mut flow_txn = FlowTransaction::new(&parent, CommitVersion(100));
///
/// // Join operator processes the event:
/// // 1. Reads source table at version 100 (snapshot)
/// let source_row = flow_txn.get(&source_key)?;
///
/// // 2. Reads join state at LATEST version (e.g., 150)
/// //    This state contains results from ALL previous CDC events
/// let join_state = flow_txn.state_get(node_id, &state_key)?;
///
/// // Without dual versions, join state would be stale at version 100
/// ```
///
/// ## Example: Distinct Operator
/// ```ignore
/// // Maintains a set of seen values across ALL CDC events
/// let seen = flow_txn.state_get(node_id, &value_key)?;
///
/// // If read at CDC version, would "forget" values seen in later events
/// // Dual-version ensures we see ALL distinct values accumulated so far
/// ```
///
/// # Current Usage Pattern
///
/// FlowTransaction is used in worker threads to process CDC batches:
///
/// ```ignore
/// // In flow worker thread
/// let primitive_query = engine.multi().begin_query_at_version(batch.version)?;
/// let state_query = engine.multi().begin_query_at_version(state_version)?;
///
/// let mut txn = FlowTransaction {
///     version: batch.version,
///     pending: PendingWrites::new(),
///     primitive_query,
///     state_query,
///     catalog: catalog.clone(),
/// };
///
/// for change in batch.changes {
///     flow_engine.process(&mut txn, change, flow_id)?;
/// }
///
/// // Extract pending writes to merge into parent transaction
/// let pending = txn.pending;
/// ```
///
/// # Write Path
///
/// All writes (`set`, `remove`) go to the local `pending` buffer:
/// - Reads check pending buffer first, then delegate to query transactions
/// - Pending writes are extracted and applied to parent transaction by caller
///
/// # Thread Safety
///
/// FlowTransaction is Send because all fields are either Copy, owned, or
pub struct FlowTransaction {
	/// CDC event version for snapshot isolation.
	///
	/// This is the version at which the CDC event was generated, NOT the parent transaction version.
	/// Source data reads see the database state as of this CDC version.
	/// This guarantees proper snapshot isolation - the flow processes data as it existed when
	/// the CDC event was created, regardless of concurrent modifications.
	pub(crate) version: CommitVersion,

	/// Local write buffer for pending changes.
	///
	/// Stores all `set()` and `remove()` operations made by this transaction.
	/// Returned to caller for application to parent transaction.
	pub(crate) pending: PendingWrites,

	/// Read-only query transaction for accessing storage primitive data at CDC snapshot version.
	///
	/// Provides snapshot reads at `version`. Used for reading storage primitives tables/views
	/// to ensure consistent view of the data being processed by the flow.
	pub(crate) primitive_query: MultiReadTransaction,

	/// Read-only query transaction for accessing flow state at latest version.
	///
	/// Reads at the latest committed version. Used for reading flow state
	/// (join tables, distinct values, counters) that must be visible across
	/// all CDC versions to maintain continuity.
	pub(crate) state_query: MultiReadTransaction,

	/// Catalog for metadata access (cloned from parent, Arc-based so cheap)
	pub(crate) catalog: Catalog,

	/// Interceptors for view data operations
	pub(crate) interceptors: Interceptors,
}

impl FlowTransaction {
	/// Create a new FlowTransaction from a parent transaction at a specific CDC version.
	///
	/// Creates dual query transactions:
	/// - `primitive_query`: Reads at the specified CDC version (snapshot isolation)
	/// - `state_query`: Reads at the latest version (state continuity)
	///
	/// # Parameters
	/// * `parent` - The parent command transaction to derive from
	/// * `version` - The CDC event version for snapshot isolation (NOT parent.version())
	/// * `catalog` - The catalog for metadata access
	#[instrument(name = "flow::transaction::new", level = "debug", skip(parent, catalog, interceptors), fields(version = version.0))]
	pub fn new(
		parent: &AdminTransaction,
		version: CommitVersion,
		catalog: Catalog,
		interceptors: Interceptors,
	) -> Self {
		let mut primitive_query = parent.multi.begin_query().unwrap();
		primitive_query.read_as_of_version_inclusive(version);

		let state_query = parent.multi.begin_query().unwrap();
		Self {
			version,
			pending: PendingWrites::new(),
			primitive_query,
			state_query,
			catalog,
			interceptors,
		}
	}

	/// Update the transaction to read at a new version
	pub fn update_version(&mut self, new_version: CommitVersion) {
		self.version = new_version;
		self.primitive_query.read_as_of_version_inclusive(new_version);
	}

	/// Get access to the catalog for reading metadata
	pub(crate) fn catalog(&self) -> &Catalog {
		&self.catalog
	}
}

impl WithInterceptors for FlowTransaction {
	fn table_pre_insert_interceptors(&mut self) -> &mut Chain<dyn TablePreInsertInterceptor + Send + Sync> {
		&mut self.interceptors.table_pre_insert
	}

	fn table_post_insert_interceptors(&mut self) -> &mut Chain<dyn TablePostInsertInterceptor + Send + Sync> {
		&mut self.interceptors.table_post_insert
	}

	fn table_pre_update_interceptors(&mut self) -> &mut Chain<dyn TablePreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.table_pre_update
	}

	fn table_post_update_interceptors(&mut self) -> &mut Chain<dyn TablePostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.table_post_update
	}

	fn table_pre_delete_interceptors(&mut self) -> &mut Chain<dyn TablePreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.table_pre_delete
	}

	fn table_post_delete_interceptors(&mut self) -> &mut Chain<dyn TablePostDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.table_post_delete
	}

	fn ringbuffer_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreInsertInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_pre_insert
	}

	fn ringbuffer_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostInsertInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_post_insert
	}

	fn ringbuffer_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_pre_update
	}

	fn ringbuffer_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_post_update
	}

	fn ringbuffer_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_pre_delete
	}

	fn ringbuffer_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_post_delete
	}

	fn pre_commit_interceptors(&mut self) -> &mut Chain<dyn PreCommitInterceptor + Send + Sync> {
		&mut self.interceptors.pre_commit
	}

	fn post_commit_interceptors(&mut self) -> &mut Chain<dyn PostCommitInterceptor + Send + Sync> {
		&mut self.interceptors.post_commit
	}

	fn namespace_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPostCreateInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_def_post_create
	}

	fn namespace_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_def_pre_update
	}

	fn namespace_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_def_post_update
	}

	fn namespace_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_def_pre_delete
	}

	fn table_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn TableDefPostCreateInterceptor + Send + Sync> {
		&mut self.interceptors.table_def_post_create
	}

	fn table_def_pre_update_interceptors(&mut self) -> &mut Chain<dyn TableDefPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.table_def_pre_update
	}

	fn table_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn TableDefPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.table_def_post_update
	}

	fn table_def_pre_delete_interceptors(&mut self) -> &mut Chain<dyn TableDefPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.table_def_pre_delete
	}

	fn view_pre_insert_interceptors(&mut self) -> &mut Chain<dyn ViewPreInsertInterceptor + Send + Sync> {
		&mut self.interceptors.view_pre_insert
	}

	fn view_post_insert_interceptors(&mut self) -> &mut Chain<dyn ViewPostInsertInterceptor + Send + Sync> {
		&mut self.interceptors.view_post_insert
	}

	fn view_pre_update_interceptors(&mut self) -> &mut Chain<dyn ViewPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.view_pre_update
	}

	fn view_post_update_interceptors(&mut self) -> &mut Chain<dyn ViewPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.view_post_update
	}

	fn view_pre_delete_interceptors(&mut self) -> &mut Chain<dyn ViewPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.view_pre_delete
	}

	fn view_post_delete_interceptors(&mut self) -> &mut Chain<dyn ViewPostDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.view_post_delete
	}

	fn view_def_post_create_interceptors(&mut self) -> &mut Chain<dyn ViewDefPostCreateInterceptor + Send + Sync> {
		&mut self.interceptors.view_def_post_create
	}

	fn view_def_pre_update_interceptors(&mut self) -> &mut Chain<dyn ViewDefPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.view_def_pre_update
	}

	fn view_def_post_update_interceptors(&mut self) -> &mut Chain<dyn ViewDefPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.view_def_post_update
	}

	fn view_def_pre_delete_interceptors(&mut self) -> &mut Chain<dyn ViewDefPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.view_def_pre_delete
	}

	fn ringbuffer_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPostCreateInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_def_post_create
	}

	fn ringbuffer_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_def_pre_update
	}

	fn ringbuffer_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_def_post_update
	}

	fn ringbuffer_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_def_pre_delete
	}
}
