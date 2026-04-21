// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, mem, sync::Arc};

use read::ReadFrom;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape},
	interface::{
		catalog::shape::ShapeId,
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_runtime::context::clock::Clock;
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

pub mod range;
pub mod read;
pub mod state;
pub mod write;

/// Parameters for creating a transactional (inline) FlowTransaction.
pub struct TransactionalParams {
	pub version: CommitVersion,
	pub pending: Pending,
	pub base_pending: Pending,
	pub query: MultiReadTransaction,
	pub state_query: MultiReadTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub clock: Clock,
	/// In-transaction view outputs accumulated by the pre-commit interceptor
	/// from previous execution levels. Operators that read from a view parent
	/// (e.g. `PrimitiveViewOperator::pull`) overlay these on top of their
	/// `read_version` storage scan so sibling transactional views are visible
	/// within the same pre-commit.
	pub view_overlay: Arc<Vec<Change>>,
}

/// Shared fields between Deferred and Transactional variants.
pub struct FlowTransactionInner {
	pub version: CommitVersion,
	pub pending: Pending,
	pub pending_shapes: Vec<RowShape>,
	pub query: MultiReadTransaction,
	pub state_query: Option<MultiReadTransaction>,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub accumulator: ChangeAccumulator,
	pub clock: Clock,
}

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
		/// View outputs produced by sibling flows in earlier execution levels
		/// of this pre-commit. Consulted by view-reading pull paths to overlay
		/// in-transaction writes on top of the `read_version` snapshot. Empty
		/// for the first level.
		view_overlay: Arc<Vec<Change>>,
	},

	/// Ephemeral subscription flow processing.
	///
	/// Operator state lives in an in-memory HashMap instead of the multi-version
	/// store; source reads go through query at the CDC version.
	/// No writes are committed to persistent storage.
	Ephemeral {
		inner: FlowTransactionInner,
		/// In-memory operator state, replacing state_query for FlowNodeState keys.
		state: HashMap<EncodedKey, EncodedRow>,
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
			}
			| Self::Ephemeral {
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
			}
			| Self::Ephemeral {
				inner,
				..
			} => inner,
		}
	}

	/// Create a deferred (CDC) FlowTransaction from a parent transaction.
	///
	/// Used by the async worker path. Reads only from committed storage +
	/// flow-generated pending writes — no base pending from a parent transaction.
	#[instrument(name = "flow::transaction::deferred", level = "debug", skip(parent, catalog, interceptors, clock), fields(version = version.0))]
	pub fn deferred(
		parent: &AdminTransaction,
		version: CommitVersion,
		catalog: Catalog,
		interceptors: Interceptors,
		clock: Clock,
	) -> Self {
		let mut query = parent.multi.begin_query().unwrap();
		query.read_as_of_version_inclusive(version);

		let state_query = parent.multi.begin_query().unwrap();
		Self::Deferred {
			inner: FlowTransactionInner {
				version,
				pending: Pending::new(),
				pending_shapes: Vec::new(),
				query,
				state_query: Some(state_query),
				catalog,
				interceptors,
				accumulator: ChangeAccumulator::new(),
				clock,
			},
		}
	}

	/// Create a deferred (CDC) FlowTransaction from pre-built parts.
	///
	/// Used by the worker actor which creates its own query transactions.
	pub fn deferred_from_parts(
		version: CommitVersion,
		pending: Pending,
		query: MultiReadTransaction,
		state_query: MultiReadTransaction,
		catalog: Catalog,
		interceptors: Interceptors,
		clock: Clock,
	) -> Self {
		Self::Deferred {
			inner: FlowTransactionInner {
				version,
				pending,
				pending_shapes: Vec::new(),
				query,
				state_query: Some(state_query),
				catalog,
				interceptors,
				accumulator: ChangeAccumulator::new(),
				clock,
			},
		}
	}

	/// Create a transactional (inline) FlowTransaction.
	///
	/// Used by the pre-commit interceptor path. `base_pending` is a read-only
	/// snapshot of the committing transaction's KV writes so that flow operators
	/// can see uncommitted row data.
	pub fn transactional(params: TransactionalParams) -> Self {
		Self::Transactional {
			inner: FlowTransactionInner {
				version: params.version,
				pending: params.pending,
				pending_shapes: Vec::new(),
				query: params.query,
				state_query: Some(params.state_query),
				catalog: params.catalog,
				interceptors: params.interceptors,
				accumulator: ChangeAccumulator::new(),
				clock: params.clock,
			},
			base_pending: params.base_pending,
			view_overlay: params.view_overlay,
		}
	}

	/// Return a (cheap) clone of the in-transaction view overlay, if any.
	/// Returns `None` for Deferred / Ephemeral transactions (which read
	/// everything from committed storage). Operators reading from a view
	/// parent overlay these changes on top of their storage reads so sibling
	/// transactional view outputs produced earlier in the same pre-commit
	/// are visible.
	pub fn view_overlay(&self) -> Option<Arc<Vec<Change>>> {
		match self {
			Self::Transactional {
				view_overlay,
				..
			} => Some(Arc::clone(view_overlay)),
			_ => None,
		}
	}

	/// Create an ephemeral (subscription) FlowTransaction.
	///
	/// Operator state is backed by an in-memory HashMap. Source data reads
	/// go through `query` at the specified version. State reads
	/// go to `state` instead of the multi-version store.
	pub fn ephemeral(
		version: CommitVersion,
		query: MultiReadTransaction,
		catalog: Catalog,
		state: HashMap<EncodedKey, EncodedRow>,
		clock: Clock,
	) -> Self {
		let mut pq = query;
		pq.read_as_of_version_inclusive(version);

		Self::Ephemeral {
			inner: FlowTransactionInner {
				version,
				pending: Pending::new(),
				pending_shapes: Vec::new(),
				query: pq,
				state_query: None,
				catalog,
				interceptors: Interceptors::new(),
				accumulator: ChangeAccumulator::new(),
				clock,
			},
			state,
		}
	}

	/// Merge pending state writes back into the ephemeral state HashMap.
	///
	/// After flow processing, pending writes contain both state mutations and
	/// subscription output writes. This method merges state mutations (keys
	/// matching FlowNodeState/FlowNodeInternalState) back into state
	/// and clears pending.
	///
	/// Only applicable to the Ephemeral variant; no-op for others.
	pub fn merge_state(&mut self) {
		if let Self::Ephemeral {
			inner,
			state,
		} = self
		{
			for (key, write) in inner.pending.iter_sorted() {
				if matches!(Self::read_from(key), ReadFrom::StateQuery) {
					match write {
						PendingWrite::Set(row) => {
							state.insert(key.clone(), row.clone());
						}
						PendingWrite::Remove => {
							state.remove(key);
						}
					}
				}
			}
			inner.pending = Pending::new();
		}
	}

	/// Extract the ephemeral state HashMap, consuming the state from this transaction.
	///
	/// Used to persist ephemeral state across CDC batches.
	/// Only applicable to the Ephemeral variant; returns empty HashMap for others.
	pub fn take_state(&mut self) -> HashMap<EncodedKey, EncodedRow> {
		if let Self::Ephemeral {
			state,
			..
		} = self
		{
			mem::take(state)
		} else {
			HashMap::new()
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
		inner.query.read_as_of_version_inclusive(new_version);
	}

	/// Get access to the catalog for reading metadata
	pub fn catalog(&self) -> &Catalog {
		&self.inner().catalog
	}

	/// Get access to the clock for timestamp generation
	pub fn clock(&self) -> &Clock {
		&self.inner().clock
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
