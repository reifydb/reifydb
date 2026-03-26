// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use pending::{Pending, PendingWrite};
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::primitive::PrimitiveId,
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_transaction::{
	change_accumulator::ChangeAccumulator,
	interceptor::{
		WithInterceptors,
		chain::InterceptorChain as Chain,
		dictionary::{
			DictionaryPostDeleteInterceptor, DictionaryPostInsertInterceptor,
			DictionaryPostUpdateInterceptor, DictionaryPreDeleteInterceptor,
			DictionaryPreInsertInterceptor, DictionaryPreUpdateInterceptor,
		},
		dictionary_def::{
			DictionaryDefPostCreateInterceptor, DictionaryDefPostUpdateInterceptor,
			DictionaryDefPreDeleteInterceptor, DictionaryDefPreUpdateInterceptor,
		},
		interceptors::Interceptors,
		namespace::{
			NamespacePostCreateInterceptor, NamespacePostUpdateInterceptor, NamespacePreDeleteInterceptor,
			NamespacePreUpdateInterceptor,
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
		series::{
			SeriesPostDeleteInterceptor, SeriesPostInsertInterceptor, SeriesPostUpdateInterceptor,
			SeriesPreDeleteInterceptor, SeriesPreInsertInterceptor, SeriesPreUpdateInterceptor,
		},
		series_def::{
			SeriesDefPostCreateInterceptor, SeriesDefPostUpdateInterceptor, SeriesDefPreDeleteInterceptor,
			SeriesDefPreUpdateInterceptor,
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

/// Shared fields between Deferred and Transactional variants.
pub struct FlowTransactionInner {
	pub version: CommitVersion,
	pub pending: Pending,
	pub primitive_query: MultiReadTransaction,
	pub state_query: MultiReadTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub accumulator: ChangeAccumulator,
}

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
		inner: FlowTransactionInner,
	},

	/// Inline flow processing within a committing transaction.
	/// Can additionally read uncommitted writes from the parent transaction.
	Transactional {
		inner: FlowTransactionInner,
		/// Read-only snapshot of the committing transaction's KV writes.
		base_pending: Pending,
	},
}

impl FlowTransaction {
	fn inner(&self) -> &FlowTransactionInner {
		match self {
			Self::Deferred {
				inner,
				..
			}
			| Self::Transactional {
				inner,
				..
			} => inner,
		}
	}

	fn inner_mut(&mut self) -> &mut FlowTransactionInner {
		match self {
			Self::Deferred {
				inner,
				..
			}
			| Self::Transactional {
				inner,
				..
			} => inner,
		}
	}

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
			inner: FlowTransactionInner {
				version,
				pending: Pending::new(),
				primitive_query,
				state_query,
				catalog,
				interceptors,
				accumulator: ChangeAccumulator::new(),
			},
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
			inner: FlowTransactionInner {
				version,
				pending,
				primitive_query,
				state_query,
				catalog,
				interceptors,
				accumulator: ChangeAccumulator::new(),
			},
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
			inner: FlowTransactionInner {
				version,
				pending,
				primitive_query,
				state_query,
				catalog,
				interceptors,
				accumulator: ChangeAccumulator::new(),
			},
			base_pending,
		}
	}

	/// Get the transaction version.
	pub fn version(&self) -> CommitVersion {
		self.inner().version
	}

	/// Extract pending writes, replacing them with an empty buffer.
	pub fn take_pending(&mut self) -> Pending {
		mem::take(&mut self.inner_mut().pending)
	}

	/// Track a view-level flow change in this transaction's accumulator.
	pub fn track_flow_change(&mut self, change: Change) {
		if let ChangeOrigin::Primitive(id) = change.origin {
			for diff in change.diffs {
				self.inner_mut().accumulator.track(id, diff);
			}
		}
	}

	/// Drain the accumulator entries collected during flow processing.
	pub fn take_accumulator_entries(&mut self) -> Vec<(PrimitiveId, Diff)> {
		let acc = &mut self.inner_mut().accumulator;
		let entries: Vec<_> = acc.entries_from(0).to_vec();
		acc.clear();
		entries
	}

	/// Get a reference to the pending writes.
	#[cfg(test)]
	pub fn pending(&self) -> &Pending {
		&self.inner().pending
	}

	/// Update the transaction to read at a new version
	pub fn update_version(&mut self, new_version: CommitVersion) {
		let inner = self.inner_mut();
		inner.version = new_version;
		inner.primitive_query.read_as_of_version_inclusive(new_version);
	}

	/// Get access to the catalog for reading metadata
	pub fn catalog(&self) -> &Catalog {
		&self.inner().catalog
	}
}

macro_rules! interceptor_method {
	($method:ident, $field:ident, $trait_name:ident) => {
		fn $method(&mut self) -> &mut Chain<dyn $trait_name + Send + Sync> {
			&mut self.inner_mut().interceptors.$field
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

	interceptor_method!(namespace_post_create_interceptors, namespace_post_create, NamespacePostCreateInterceptor);
	interceptor_method!(namespace_pre_update_interceptors, namespace_pre_update, NamespacePreUpdateInterceptor);
	interceptor_method!(namespace_post_update_interceptors, namespace_post_update, NamespacePostUpdateInterceptor);
	interceptor_method!(namespace_pre_delete_interceptors, namespace_pre_delete, NamespacePreDeleteInterceptor);

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

	interceptor_method!(dictionary_pre_insert_interceptors, dictionary_pre_insert, DictionaryPreInsertInterceptor);
	interceptor_method!(
		dictionary_post_insert_interceptors,
		dictionary_post_insert,
		DictionaryPostInsertInterceptor
	);
	interceptor_method!(dictionary_pre_update_interceptors, dictionary_pre_update, DictionaryPreUpdateInterceptor);
	interceptor_method!(
		dictionary_post_update_interceptors,
		dictionary_post_update,
		DictionaryPostUpdateInterceptor
	);
	interceptor_method!(dictionary_pre_delete_interceptors, dictionary_pre_delete, DictionaryPreDeleteInterceptor);
	interceptor_method!(
		dictionary_post_delete_interceptors,
		dictionary_post_delete,
		DictionaryPostDeleteInterceptor
	);

	interceptor_method!(
		dictionary_def_post_create_interceptors,
		dictionary_def_post_create,
		DictionaryDefPostCreateInterceptor
	);
	interceptor_method!(
		dictionary_def_pre_update_interceptors,
		dictionary_def_pre_update,
		DictionaryDefPreUpdateInterceptor
	);
	interceptor_method!(
		dictionary_def_post_update_interceptors,
		dictionary_def_post_update,
		DictionaryDefPostUpdateInterceptor
	);
	interceptor_method!(
		dictionary_def_pre_delete_interceptors,
		dictionary_def_pre_delete,
		DictionaryDefPreDeleteInterceptor
	);

	interceptor_method!(series_pre_insert_interceptors, series_pre_insert, SeriesPreInsertInterceptor);
	interceptor_method!(series_post_insert_interceptors, series_post_insert, SeriesPostInsertInterceptor);
	interceptor_method!(series_pre_update_interceptors, series_pre_update, SeriesPreUpdateInterceptor);
	interceptor_method!(series_post_update_interceptors, series_post_update, SeriesPostUpdateInterceptor);
	interceptor_method!(series_pre_delete_interceptors, series_pre_delete, SeriesPreDeleteInterceptor);
	interceptor_method!(series_post_delete_interceptors, series_post_delete, SeriesPostDeleteInterceptor);

	interceptor_method!(
		series_def_post_create_interceptors,
		series_def_post_create,
		SeriesDefPostCreateInterceptor
	);
	interceptor_method!(series_def_pre_update_interceptors, series_def_pre_update, SeriesDefPreUpdateInterceptor);
	interceptor_method!(
		series_def_post_update_interceptors,
		series_def_post_update,
		SeriesDefPostUpdateInterceptor
	);
	interceptor_method!(series_def_pre_delete_interceptors, series_def_pre_delete, SeriesDefPreDeleteInterceptor);
}
