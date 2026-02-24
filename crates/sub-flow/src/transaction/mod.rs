// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use pending::{Pending, PendingWrite};
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
///          ├──► pending (flow-generated writes)
///          │
///          ├──► variant
///          │    ├─ Deferred: skip
///          │    └─ Transactional { base_pending }: check base_pending
///          │
///          ├──► primitive_query (at CDC version)
///          │    - Source tables / views / regular data
///          │
///          └──► state_query (at latest version)
///               - FlowNodeState / FlowNodeInternalState
/// ```
///
/// # Construction
///
/// Use named constructors to enforce correct initialization:
/// - [`FlowTransaction::deferred`] — CDC path (no base pending)
/// - [`FlowTransaction::transactional`] — inline pre-commit path (with base pending)
///
/// # Write Path
///
/// All writes (`set`, `remove`) go to the local `pending` buffer:
/// - Reads check pending buffer first, then delegate to query transactions
/// - Pending writes are extracted via [`FlowTransaction::take_pending`]
///
/// # Thread Safety
///
/// FlowTransaction is Send because all fields are either Copy, owned, or
pub enum FlowTransaction {
	/// CDC-driven async flow processing.
	/// Reads only from committed storage + flow pending writes.
	Deferred {
		version: CommitVersion,
		pending: Pending,
		primitive_query: MultiReadTransaction,
		state_query: MultiReadTransaction,
		catalog: Catalog,
		interceptors: Interceptors,
	},

	/// Inline flow processing within a committing transaction.
	/// Can additionally read uncommitted writes from the parent transaction.
	Transactional {
		version: CommitVersion,
		pending: Pending,
		/// Read-only snapshot of the committing transaction's KV writes.
		base_pending: Pending,
		primitive_query: MultiReadTransaction,
		state_query: MultiReadTransaction,
		catalog: Catalog,
		interceptors: Interceptors,
	},
}

impl FlowTransaction {
	/// Create a deferred (CDC) FlowTransaction from a parent transaction.
	///
	/// Used by the async worker path. Reads only from committed storage +
	/// flow-generated pending writes — no base pending from a parent transaction.
	#[instrument(name = "flow::transaction::deferred", level = "debug", skip(parent, catalog, interceptors), fields(version = version.0))]
	pub fn deferred(
		parent: &AdminTransaction,
		version: CommitVersion,
		catalog: Catalog,
		interceptors: Interceptors,
	) -> Self {
		let mut primitive_query = parent.multi.begin_query().unwrap();
		primitive_query.read_as_of_version_inclusive(version);

		let state_query = parent.multi.begin_query().unwrap();
		Self::Deferred {
			version,
			pending: Pending::new(),
			primitive_query,
			state_query,
			catalog,
			interceptors,
		}
	}

	/// Create a deferred (CDC) FlowTransaction from pre-built parts.
	///
	/// Used by the worker actor which creates its own query transactions.
	pub fn deferred_from_parts(
		version: CommitVersion,
		pending: Pending,
		primitive_query: MultiReadTransaction,
		state_query: MultiReadTransaction,
		catalog: Catalog,
		interceptors: Interceptors,
	) -> Self {
		Self::Deferred {
			version,
			pending,
			primitive_query,
			state_query,
			catalog,
			interceptors,
		}
	}

	/// Create a transactional (inline) FlowTransaction.
	///
	/// Used by the pre-commit interceptor path. `base_pending` is a read-only
	/// snapshot of the committing transaction's KV writes so that flow operators
	/// can see uncommitted row data.
	pub fn transactional(
		version: CommitVersion,
		pending: Pending,
		base_pending: Pending,
		primitive_query: MultiReadTransaction,
		state_query: MultiReadTransaction,
		catalog: Catalog,
		interceptors: Interceptors,
	) -> Self {
		Self::Transactional {
			version,
			pending,
			base_pending,
			primitive_query,
			state_query,
			catalog,
			interceptors,
		}
	}

	/// Get the transaction version.
	pub fn version(&self) -> CommitVersion {
		match self {
			Self::Deferred {
				version,
				..
			} => *version,
			Self::Transactional {
				version,
				..
			} => *version,
		}
	}

	/// Extract pending writes, replacing them with an empty buffer.
	pub fn take_pending(&mut self) -> Pending {
		match self {
			Self::Deferred {
				pending,
				..
			} => std::mem::take(pending),
			Self::Transactional {
				pending,
				..
			} => std::mem::take(pending),
		}
	}

	/// Get a reference to the pending writes.
	#[cfg(test)]
	pub fn pending(&self) -> &Pending {
		match self {
			Self::Deferred {
				pending,
				..
			} => pending,
			Self::Transactional {
				pending,
				..
			} => pending,
		}
	}

	/// Drain all generated view changes, returning them.
	pub fn take_view_changes(&mut self) -> pending::ViewChanges {
		match self {
			Self::Deferred {
				pending,
				..
			} => pending.take_view_changes(),
			Self::Transactional {
				pending,
				..
			} => pending.take_view_changes(),
		}
	}

	/// Append a view change (used by `SinkViewOperator`).
	pub fn push_view_change(&mut self, change: reifydb_core::interface::change::Change) {
		match self {
			Self::Deferred {
				pending,
				..
			} => pending.push_view_change(change),
			Self::Transactional {
				pending,
				..
			} => pending.push_view_change(change),
		}
	}

	/// Update the transaction to read at a new version
	pub fn update_version(&mut self, new_version: CommitVersion) {
		match self {
			Self::Deferred {
				version,
				primitive_query,
				..
			} => {
				*version = new_version;
				primitive_query.read_as_of_version_inclusive(new_version);
			}
			Self::Transactional {
				version,
				primitive_query,
				..
			} => {
				*version = new_version;
				primitive_query.read_as_of_version_inclusive(new_version);
			}
		}
	}

	/// Get access to the catalog for reading metadata
	pub(crate) fn catalog(&self) -> &Catalog {
		match self {
			Self::Deferred {
				catalog,
				..
			} => catalog,
			Self::Transactional {
				catalog,
				..
			} => catalog,
		}
	}
}

macro_rules! interceptor_method {
	($method:ident, $field:ident, $trait_name:ident) => {
		fn $method(&mut self) -> &mut Chain<dyn $trait_name + Send + Sync> {
			match self {
				Self::Deferred {
					interceptors,
					..
				} => &mut interceptors.$field,
				Self::Transactional {
					interceptors,
					..
				} => &mut interceptors.$field,
			}
		}
	};
}

impl WithInterceptors for FlowTransaction {
	interceptor_method!(table_pre_insert_interceptors, table_pre_insert, TablePreInsertInterceptor);
	interceptor_method!(table_post_insert_interceptors, table_post_insert, TablePostInsertInterceptor);
	interceptor_method!(table_pre_update_interceptors, table_pre_update, TablePreUpdateInterceptor);
	interceptor_method!(table_post_update_interceptors, table_post_update, TablePostUpdateInterceptor);
	interceptor_method!(table_pre_delete_interceptors, table_pre_delete, TablePreDeleteInterceptor);
	interceptor_method!(table_post_delete_interceptors, table_post_delete, TablePostDeleteInterceptor);

	interceptor_method!(ringbuffer_pre_insert_interceptors, ringbuffer_pre_insert, RingBufferPreInsertInterceptor);
	interceptor_method!(
		ringbuffer_post_insert_interceptors,
		ringbuffer_post_insert,
		RingBufferPostInsertInterceptor
	);
	interceptor_method!(ringbuffer_pre_update_interceptors, ringbuffer_pre_update, RingBufferPreUpdateInterceptor);
	interceptor_method!(
		ringbuffer_post_update_interceptors,
		ringbuffer_post_update,
		RingBufferPostUpdateInterceptor
	);
	interceptor_method!(ringbuffer_pre_delete_interceptors, ringbuffer_pre_delete, RingBufferPreDeleteInterceptor);
	interceptor_method!(
		ringbuffer_post_delete_interceptors,
		ringbuffer_post_delete,
		RingBufferPostDeleteInterceptor
	);

	interceptor_method!(pre_commit_interceptors, pre_commit, PreCommitInterceptor);
	interceptor_method!(post_commit_interceptors, post_commit, PostCommitInterceptor);

	interceptor_method!(
		namespace_def_post_create_interceptors,
		namespace_def_post_create,
		NamespaceDefPostCreateInterceptor
	);
	interceptor_method!(
		namespace_def_pre_update_interceptors,
		namespace_def_pre_update,
		NamespaceDefPreUpdateInterceptor
	);
	interceptor_method!(
		namespace_def_post_update_interceptors,
		namespace_def_post_update,
		NamespaceDefPostUpdateInterceptor
	);
	interceptor_method!(
		namespace_def_pre_delete_interceptors,
		namespace_def_pre_delete,
		NamespaceDefPreDeleteInterceptor
	);

	interceptor_method!(table_def_post_create_interceptors, table_def_post_create, TableDefPostCreateInterceptor);
	interceptor_method!(table_def_pre_update_interceptors, table_def_pre_update, TableDefPreUpdateInterceptor);
	interceptor_method!(table_def_post_update_interceptors, table_def_post_update, TableDefPostUpdateInterceptor);
	interceptor_method!(table_def_pre_delete_interceptors, table_def_pre_delete, TableDefPreDeleteInterceptor);

	interceptor_method!(view_pre_insert_interceptors, view_pre_insert, ViewPreInsertInterceptor);
	interceptor_method!(view_post_insert_interceptors, view_post_insert, ViewPostInsertInterceptor);
	interceptor_method!(view_pre_update_interceptors, view_pre_update, ViewPreUpdateInterceptor);
	interceptor_method!(view_post_update_interceptors, view_post_update, ViewPostUpdateInterceptor);
	interceptor_method!(view_pre_delete_interceptors, view_pre_delete, ViewPreDeleteInterceptor);
	interceptor_method!(view_post_delete_interceptors, view_post_delete, ViewPostDeleteInterceptor);

	interceptor_method!(view_def_post_create_interceptors, view_def_post_create, ViewDefPostCreateInterceptor);
	interceptor_method!(view_def_pre_update_interceptors, view_def_pre_update, ViewDefPreUpdateInterceptor);
	interceptor_method!(view_def_post_update_interceptors, view_def_post_update, ViewDefPostUpdateInterceptor);
	interceptor_method!(view_def_pre_delete_interceptors, view_def_pre_delete, ViewDefPreDeleteInterceptor);

	interceptor_method!(
		ringbuffer_def_post_create_interceptors,
		ringbuffer_def_post_create,
		RingBufferDefPostCreateInterceptor
	);
	interceptor_method!(
		ringbuffer_def_pre_update_interceptors,
		ringbuffer_def_pre_update,
		RingBufferDefPreUpdateInterceptor
	);
	interceptor_method!(
		ringbuffer_def_post_update_interceptors,
		ringbuffer_def_post_update,
		RingBufferDefPostUpdateInterceptor
	);
	interceptor_method!(
		ringbuffer_def_pre_delete_interceptors,
		ringbuffer_def_pre_delete,
		RingBufferDefPreDeleteInterceptor
	);
}
