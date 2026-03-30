// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem;

use pending::{Pending, PendingWrite};
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::CommitVersion,
	encoded::shape::RowShape,
	interface::{
		catalog::shape::ShapeId,
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_transaction::{
	change_accumulator::ChangeAccumulator,
	interceptor::{
		WithInterceptors,
		authentication::{AuthenticationPostCreateInterceptor, AuthenticationPreDeleteInterceptor},
		chain::InterceptorChain as Chain,
		dictionary::{
			DictionaryPostCreateInterceptor, DictionaryPostUpdateInterceptor,
			DictionaryPreDeleteInterceptor, DictionaryPreUpdateInterceptor,
		},
		dictionary_row::{
			DictionaryRowPostDeleteInterceptor, DictionaryRowPostInsertInterceptor,
			DictionaryRowPostUpdateInterceptor, DictionaryRowPreDeleteInterceptor,
			DictionaryRowPreInsertInterceptor, DictionaryRowPreUpdateInterceptor,
		},
		granted_role::{GrantedRolePostCreateInterceptor, GrantedRolePreDeleteInterceptor},
		identity::{
			IdentityPostCreateInterceptor, IdentityPostUpdateInterceptor, IdentityPreDeleteInterceptor,
			IdentityPreUpdateInterceptor,
		},
		interceptors::Interceptors,
		namespace::{
			NamespacePostCreateInterceptor, NamespacePostUpdateInterceptor, NamespacePreDeleteInterceptor,
			NamespacePreUpdateInterceptor,
		},
		ringbuffer::{
			RingBufferPostCreateInterceptor, RingBufferPostUpdateInterceptor,
			RingBufferPreDeleteInterceptor, RingBufferPreUpdateInterceptor,
		},
		ringbuffer_row::{
			RingBufferRowPostDeleteInterceptor, RingBufferRowPostInsertInterceptor,
			RingBufferRowPostUpdateInterceptor, RingBufferRowPreDeleteInterceptor,
			RingBufferRowPreInsertInterceptor, RingBufferRowPreUpdateInterceptor,
		},
		role::{
			RolePostCreateInterceptor, RolePostUpdateInterceptor, RolePreDeleteInterceptor,
			RolePreUpdateInterceptor,
		},
		series::{
			SeriesPostCreateInterceptor, SeriesPostUpdateInterceptor, SeriesPreDeleteInterceptor,
			SeriesPreUpdateInterceptor,
		},
		series_row::{
			SeriesRowPostDeleteInterceptor, SeriesRowPostInsertInterceptor, SeriesRowPostUpdateInterceptor,
			SeriesRowPreDeleteInterceptor, SeriesRowPreInsertInterceptor, SeriesRowPreUpdateInterceptor,
		},
		table::{
			TablePostCreateInterceptor, TablePostUpdateInterceptor, TablePreDeleteInterceptor,
			TablePreUpdateInterceptor,
		},
		table_row::{
			TableRowPostDeleteInterceptor, TableRowPostInsertInterceptor, TableRowPostUpdateInterceptor,
			TableRowPreDeleteInterceptor, TableRowPreInsertInterceptor, TableRowPreUpdateInterceptor,
		},
		transaction::{PostCommitInterceptor, PreCommitInterceptor},
		view::{
			ViewPostCreateInterceptor, ViewPostUpdateInterceptor, ViewPreDeleteInterceptor,
			ViewPreUpdateInterceptor,
		},
		view_row::{
			ViewRowPostDeleteInterceptor, ViewRowPostInsertInterceptor, ViewRowPostUpdateInterceptor,
			ViewRowPreDeleteInterceptor, ViewRowPreInsertInterceptor, ViewRowPreUpdateInterceptor,
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
	pub pending_shapes: Vec<RowShape>,
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

	pub(crate) fn inner_mut(&mut self) -> &mut FlowTransactionInner {
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
				pending_shapes: Vec::new(),
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
				pending_shapes: Vec::new(),
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
				pending_shapes: Vec::new(),
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

	/// Extract pending shapes, replacing them with an empty buffer.
	pub fn take_pending_shapes(&mut self) -> Vec<RowShape> {
		mem::take(&mut self.inner_mut().pending_shapes)
	}

	/// Track a view-level flow change in this transaction's accumulator.
	pub fn track_flow_change(&mut self, change: Change) {
		if let ChangeOrigin::Shape(id) = change.origin {
			for diff in change.diffs {
				self.inner_mut().accumulator.track(id, diff);
			}
		}
	}

	/// Drain the accumulator entries collected during flow processing.
	pub fn take_accumulator_entries(&mut self) -> Vec<(ShapeId, Diff)> {
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
	interceptor_method!(table_row_pre_insert_interceptors, table_row_pre_insert, TableRowPreInsertInterceptor);
	interceptor_method!(table_row_post_insert_interceptors, table_row_post_insert, TableRowPostInsertInterceptor);
	interceptor_method!(table_row_pre_update_interceptors, table_row_pre_update, TableRowPreUpdateInterceptor);
	interceptor_method!(table_row_post_update_interceptors, table_row_post_update, TableRowPostUpdateInterceptor);
	interceptor_method!(table_row_pre_delete_interceptors, table_row_pre_delete, TableRowPreDeleteInterceptor);
	interceptor_method!(table_row_post_delete_interceptors, table_row_post_delete, TableRowPostDeleteInterceptor);

	interceptor_method!(
		ringbuffer_row_pre_insert_interceptors,
		ringbuffer_row_pre_insert,
		RingBufferRowPreInsertInterceptor
	);
	interceptor_method!(
		ringbuffer_row_post_insert_interceptors,
		ringbuffer_row_post_insert,
		RingBufferRowPostInsertInterceptor
	);
	interceptor_method!(
		ringbuffer_row_pre_update_interceptors,
		ringbuffer_row_pre_update,
		RingBufferRowPreUpdateInterceptor
	);
	interceptor_method!(
		ringbuffer_row_post_update_interceptors,
		ringbuffer_row_post_update,
		RingBufferRowPostUpdateInterceptor
	);
	interceptor_method!(
		ringbuffer_row_pre_delete_interceptors,
		ringbuffer_row_pre_delete,
		RingBufferRowPreDeleteInterceptor
	);
	interceptor_method!(
		ringbuffer_row_post_delete_interceptors,
		ringbuffer_row_post_delete,
		RingBufferRowPostDeleteInterceptor
	);

	interceptor_method!(pre_commit_interceptors, pre_commit, PreCommitInterceptor);
	interceptor_method!(post_commit_interceptors, post_commit, PostCommitInterceptor);

	interceptor_method!(namespace_post_create_interceptors, namespace_post_create, NamespacePostCreateInterceptor);
	interceptor_method!(namespace_pre_update_interceptors, namespace_pre_update, NamespacePreUpdateInterceptor);
	interceptor_method!(namespace_post_update_interceptors, namespace_post_update, NamespacePostUpdateInterceptor);
	interceptor_method!(namespace_pre_delete_interceptors, namespace_pre_delete, NamespacePreDeleteInterceptor);

	interceptor_method!(table_post_create_interceptors, table_post_create, TablePostCreateInterceptor);
	interceptor_method!(table_pre_update_interceptors, table_pre_update, TablePreUpdateInterceptor);
	interceptor_method!(table_post_update_interceptors, table_post_update, TablePostUpdateInterceptor);
	interceptor_method!(table_pre_delete_interceptors, table_pre_delete, TablePreDeleteInterceptor);

	interceptor_method!(view_row_pre_insert_interceptors, view_row_pre_insert, ViewRowPreInsertInterceptor);
	interceptor_method!(view_row_post_insert_interceptors, view_row_post_insert, ViewRowPostInsertInterceptor);
	interceptor_method!(view_row_pre_update_interceptors, view_row_pre_update, ViewRowPreUpdateInterceptor);
	interceptor_method!(view_row_post_update_interceptors, view_row_post_update, ViewRowPostUpdateInterceptor);
	interceptor_method!(view_row_pre_delete_interceptors, view_row_pre_delete, ViewRowPreDeleteInterceptor);
	interceptor_method!(view_row_post_delete_interceptors, view_row_post_delete, ViewRowPostDeleteInterceptor);

	interceptor_method!(view_post_create_interceptors, view_post_create, ViewPostCreateInterceptor);
	interceptor_method!(view_pre_update_interceptors, view_pre_update, ViewPreUpdateInterceptor);
	interceptor_method!(view_post_update_interceptors, view_post_update, ViewPostUpdateInterceptor);
	interceptor_method!(view_pre_delete_interceptors, view_pre_delete, ViewPreDeleteInterceptor);

	interceptor_method!(
		ringbuffer_post_create_interceptors,
		ringbuffer_post_create,
		RingBufferPostCreateInterceptor
	);
	interceptor_method!(ringbuffer_pre_update_interceptors, ringbuffer_pre_update, RingBufferPreUpdateInterceptor);
	interceptor_method!(
		ringbuffer_post_update_interceptors,
		ringbuffer_post_update,
		RingBufferPostUpdateInterceptor
	);
	interceptor_method!(ringbuffer_pre_delete_interceptors, ringbuffer_pre_delete, RingBufferPreDeleteInterceptor);

	interceptor_method!(
		dictionary_row_pre_insert_interceptors,
		dictionary_row_pre_insert,
		DictionaryRowPreInsertInterceptor
	);
	interceptor_method!(
		dictionary_row_post_insert_interceptors,
		dictionary_row_post_insert,
		DictionaryRowPostInsertInterceptor
	);
	interceptor_method!(
		dictionary_row_pre_update_interceptors,
		dictionary_row_pre_update,
		DictionaryRowPreUpdateInterceptor
	);
	interceptor_method!(
		dictionary_row_post_update_interceptors,
		dictionary_row_post_update,
		DictionaryRowPostUpdateInterceptor
	);
	interceptor_method!(
		dictionary_row_pre_delete_interceptors,
		dictionary_row_pre_delete,
		DictionaryRowPreDeleteInterceptor
	);
	interceptor_method!(
		dictionary_row_post_delete_interceptors,
		dictionary_row_post_delete,
		DictionaryRowPostDeleteInterceptor
	);

	interceptor_method!(
		dictionary_post_create_interceptors,
		dictionary_post_create,
		DictionaryPostCreateInterceptor
	);
	interceptor_method!(dictionary_pre_update_interceptors, dictionary_pre_update, DictionaryPreUpdateInterceptor);
	interceptor_method!(
		dictionary_post_update_interceptors,
		dictionary_post_update,
		DictionaryPostUpdateInterceptor
	);
	interceptor_method!(dictionary_pre_delete_interceptors, dictionary_pre_delete, DictionaryPreDeleteInterceptor);

	interceptor_method!(series_row_pre_insert_interceptors, series_row_pre_insert, SeriesRowPreInsertInterceptor);
	interceptor_method!(
		series_row_post_insert_interceptors,
		series_row_post_insert,
		SeriesRowPostInsertInterceptor
	);
	interceptor_method!(series_row_pre_update_interceptors, series_row_pre_update, SeriesRowPreUpdateInterceptor);
	interceptor_method!(
		series_row_post_update_interceptors,
		series_row_post_update,
		SeriesRowPostUpdateInterceptor
	);
	interceptor_method!(series_row_pre_delete_interceptors, series_row_pre_delete, SeriesRowPreDeleteInterceptor);
	interceptor_method!(
		series_row_post_delete_interceptors,
		series_row_post_delete,
		SeriesRowPostDeleteInterceptor
	);

	interceptor_method!(series_post_create_interceptors, series_post_create, SeriesPostCreateInterceptor);
	interceptor_method!(series_pre_update_interceptors, series_pre_update, SeriesPreUpdateInterceptor);
	interceptor_method!(series_post_update_interceptors, series_post_update, SeriesPostUpdateInterceptor);
	interceptor_method!(series_pre_delete_interceptors, series_pre_delete, SeriesPreDeleteInterceptor);
	interceptor_method!(identity_post_create_interceptors, identity_post_create, IdentityPostCreateInterceptor);
	interceptor_method!(identity_pre_update_interceptors, identity_pre_update, IdentityPreUpdateInterceptor);
	interceptor_method!(identity_post_update_interceptors, identity_post_update, IdentityPostUpdateInterceptor);
	interceptor_method!(identity_pre_delete_interceptors, identity_pre_delete, IdentityPreDeleteInterceptor);
	interceptor_method!(role_post_create_interceptors, role_post_create, RolePostCreateInterceptor);
	interceptor_method!(role_pre_update_interceptors, role_pre_update, RolePreUpdateInterceptor);
	interceptor_method!(role_post_update_interceptors, role_post_update, RolePostUpdateInterceptor);
	interceptor_method!(role_pre_delete_interceptors, role_pre_delete, RolePreDeleteInterceptor);
	interceptor_method!(
		granted_role_post_create_interceptors,
		granted_role_post_create,
		GrantedRolePostCreateInterceptor
	);
	interceptor_method!(
		granted_role_pre_delete_interceptors,
		granted_role_pre_delete,
		GrantedRolePreDeleteInterceptor
	);
	interceptor_method!(
		authentication_post_create_interceptors,
		authentication_post_create,
		AuthenticationPostCreateInterceptor
	);
	interceptor_method!(
		authentication_pre_delete_interceptors,
		authentication_pre_delete,
		AuthenticationPreDeleteInterceptor
	);
}
