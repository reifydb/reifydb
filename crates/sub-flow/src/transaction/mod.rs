// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::Any, collections::HashMap, mem, sync::Arc};

use read::ReadFrom;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	encoded::{row::EncodedRow, shape::RowShape},
	key::encoded::EncodedKey,
};
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowNodeId, shape::ShapeId},
		change::{Change, ChangeOrigin, Diff},
	},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	change_accumulator::ChangeAccumulator,
	dictionary::DictionaryAllocatorRegistry,
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
		identity_attribute::{IdentityAttributePostCreateInterceptor, IdentityAttributePreDeleteInterceptor},
		identity_attribute_value::{
			IdentityAttributeValuePostCreateInterceptor, IdentityAttributeValuePreDeleteInterceptor,
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
	},
	multi::transaction::read::MultiReadTransaction,
	single::SingleTransaction,
	transaction::{admin::AdminTransaction, command::CommandTransaction},
};
use reifydb_value::Result;
use tracing::instrument;

pub mod allocators;
pub mod dictionary;
pub mod read;
pub mod row_allocator;
pub mod slot;
pub mod state;
pub mod write;

use allocators::FlowAllocators;
use row_allocator::RowAllocatorRegistry;
use slot::{OperatorStateSlot, PersistFn};

use crate::host::{HostCatalog, StandardHostCatalog};

pub struct TransactionalParams {
	pub version: CommitVersion,
	pub pending: Pending,
	pub base_pending: Pending,
	pub query: MultiReadTransaction,
	pub state_query: MultiReadTransaction,
	pub single: SingleTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub clock: Clock,

	pub view_overlay: Arc<Vec<Change>>,

	pub allocators: FlowAllocators,
}

pub struct DeferredParams {
	pub version: CommitVersion,
	pub pending: Pending,
	pub base_pending: Arc<Pending>,
	pub query: MultiReadTransaction,
	pub state_query: MultiReadTransaction,
	// Read source for dictionary interning state, resolved against the latest committed
	// version rather than the per-item source-version `query` snapshot. `None` falls back to
	// `query` (used by auxiliary/test paths that never intern across batches).
	pub dictionary_query: Option<MultiReadTransaction>,
	pub single: SingleTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub clock: Clock,

	pub allocators: FlowAllocators,
}

pub struct CommittingParams {
	pub cmd: CommandTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub clock: Clock,

	pub allocators: FlowAllocators,
}

pub struct FlowTransactionInner {
	pub version: CommitVersion,
	pub pending: Pending,
	pub base_pending: Arc<Pending>,
	pub pending_shapes: Vec<RowShape>,
	pub query: MultiReadTransaction,
	pub state_query: Option<MultiReadTransaction>,
	pub dictionary_query: Option<MultiReadTransaction>,
	pub single: SingleTransaction,
	pub catalog: Catalog,
	pub host_catalog: Arc<dyn HostCatalog>,
	pub interceptors: Interceptors,
	pub accumulator: ChangeAccumulator,
	pub clock: Clock,

	pub operator_states: HashMap<FlowNodeId, OperatorStateSlot>,

	pub prefetch: HashMap<EncodedKey, Option<EncodedRow>>,

	pub allocators: FlowAllocators,
}

pub enum FlowTransaction {
	Deferred {
		inner: FlowTransactionInner,
	},

	Transactional {
		inner: FlowTransactionInner,

		view_overlay: Arc<Vec<Change>>,
	},

	Ephemeral {
		inner: FlowTransactionInner,

		state: HashMap<EncodedKey, EncodedRow>,
	},

	Committing {
		inner: FlowTransactionInner,

		cmd: Box<CommandTransaction>,
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
			}
			| Self::Committing {
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
			}
			| Self::Committing {
				inner,
				..
			} => inner,
		}
	}

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
		// Dictionary state reads resolve against the latest committed version (unpinned),
		// not the source-version `query`. See ReadFrom::DictionaryQuery.
		let dictionary_query = parent.multi.begin_query().unwrap();
		Self::Deferred {
			inner: FlowTransactionInner {
				version,
				pending: Pending::new(),
				base_pending: Arc::new(Pending::new()),
				pending_shapes: Vec::new(),
				query,
				state_query: Some(state_query),
				dictionary_query: Some(dictionary_query),
				single: parent.single.clone(),
				catalog: catalog.clone(),
				host_catalog: Arc::new(StandardHostCatalog::new(catalog)),
				interceptors,
				accumulator: ChangeAccumulator::new(),
				clock,
				operator_states: HashMap::new(),
				prefetch: HashMap::new(),
				allocators: FlowAllocators::new(),
			},
		}
	}

	pub fn deferred_from_parts(params: DeferredParams) -> Self {
		let mut query = params.query;
		query.read_as_of_version_inclusive(params.version);
		let state_query = params.state_query;
		let dictionary_query = params.dictionary_query;

		Self::Deferred {
			inner: FlowTransactionInner {
				version: params.version,
				pending: params.pending,
				base_pending: params.base_pending,
				pending_shapes: Vec::new(),
				query,
				state_query: Some(state_query),
				dictionary_query,
				single: params.single,
				catalog: params.catalog.clone(),
				host_catalog: Arc::new(StandardHostCatalog::new(params.catalog)),
				interceptors: params.interceptors,
				accumulator: ChangeAccumulator::new(),
				clock: params.clock,
				operator_states: HashMap::new(),
				prefetch: HashMap::new(),
				allocators: params.allocators,
			},
		}
	}

	pub fn committing(mut params: CommittingParams) -> Result<Self> {
		params.cmd.disable_conflict_tracking()?;
		let version = params.cmd.version();
		let mut query = params.cmd.multi.begin_query()?;
		query.read_as_of_version_inclusive(version);
		let mut state_query = params.cmd.multi.begin_query()?;
		state_query.read_as_of_version_inclusive(version);
		let single = params.cmd.single.clone();

		Ok(Self::Committing {
			inner: FlowTransactionInner {
				version,
				pending: Pending::new(),
				base_pending: Arc::new(Pending::new()),
				pending_shapes: Vec::new(),
				query,
				state_query: Some(state_query),
				dictionary_query: None,
				single,
				catalog: params.catalog.clone(),
				host_catalog: Arc::new(StandardHostCatalog::new(params.catalog)),
				interceptors: params.interceptors,
				accumulator: ChangeAccumulator::new(),
				clock: params.clock,
				operator_states: HashMap::new(),
				prefetch: HashMap::new(),
				allocators: params.allocators,
			},
			cmd: Box::new(params.cmd),
		})
	}

	pub fn commit(self) -> Result<CommitVersion> {
		match self {
			Self::Committing {
				mut cmd,
				..
			} => cmd.commit_unchecked(),
			_ => panic!("FlowTransaction::commit only valid on Committing variant"),
		}
	}

	pub fn transactional(params: TransactionalParams) -> Self {
		Self::Transactional {
			inner: FlowTransactionInner {
				version: params.version,
				pending: params.pending,
				base_pending: Arc::new(params.base_pending),
				pending_shapes: Vec::new(),
				query: params.query,
				state_query: Some(params.state_query),
				dictionary_query: None,
				single: params.single,
				catalog: params.catalog.clone(),
				host_catalog: Arc::new(StandardHostCatalog::new(params.catalog)),
				interceptors: params.interceptors,
				accumulator: ChangeAccumulator::new(),
				clock: params.clock,
				operator_states: HashMap::new(),
				prefetch: HashMap::new(),
				allocators: params.allocators,
			},
			view_overlay: params.view_overlay,
		}
	}

	pub fn row_allocators(&self) -> RowAllocatorRegistry {
		self.inner().allocators.row.clone()
	}

	pub fn dictionary_allocators(&self) -> DictionaryAllocatorRegistry {
		self.inner().allocators.dictionary.clone()
	}

	pub fn view_overlay(&self) -> Option<Arc<Vec<Change>>> {
		match self {
			Self::Transactional {
				view_overlay,
				..
			} => Some(Arc::clone(view_overlay)),
			_ => None,
		}
	}

	pub fn ephemeral(
		version: CommitVersion,
		query: MultiReadTransaction,
		single: SingleTransaction,
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
				base_pending: Arc::new(Pending::new()),
				pending_shapes: Vec::new(),
				query: pq,
				state_query: None,
				dictionary_query: None,
				single,
				catalog: catalog.clone(),
				host_catalog: Arc::new(StandardHostCatalog::new(catalog)),
				interceptors: Interceptors::new(),
				accumulator: ChangeAccumulator::new(),
				clock,
				operator_states: HashMap::new(),
				prefetch: HashMap::new(),
				allocators: FlowAllocators::new(),
			},
			state,
		}
	}

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
						PendingWrite::Remove | PendingWrite::Drop => {
							state.remove(key);
						}
					}
				}
			}
			inner.pending = Pending::new();
		}
	}

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

	pub fn version(&self) -> CommitVersion {
		self.inner().version
	}

	pub fn take_pending(&mut self) -> Pending {
		mem::take(&mut self.inner_mut().pending)
	}

	pub fn take_pending_shapes(&mut self) -> Vec<RowShape> {
		mem::take(&mut self.inner_mut().pending_shapes)
	}

	pub fn track_flow_change(&mut self, change: Change) {
		if let ChangeOrigin::Shape(id) = change.origin {
			for diff in change.diffs {
				self.inner_mut().accumulator.track(id, diff);
			}
		}
	}

	pub fn take_accumulator_entries(&mut self) -> Vec<(ShapeId, Diff)> {
		let acc = &mut self.inner_mut().accumulator;
		let entries: Vec<_> = acc.entries_from(0).to_vec();
		acc.clear();
		entries
	}

	pub(crate) fn pending(&self) -> &Pending {
		&self.inner().pending
	}

	pub fn update_version(&mut self, new_version: CommitVersion) {
		let inner = self.inner_mut();
		inner.version = new_version;
		inner.query.read_as_of_version_inclusive(new_version);
	}

	pub fn catalog(&self) -> &Catalog {
		&self.inner().catalog
	}

	pub fn host_catalog(&self) -> &dyn HostCatalog {
		&*self.inner().host_catalog
	}

	pub fn clock(&self) -> &Clock {
		&self.inner().clock
	}

	pub fn operator_state<S, F>(&mut self, node: FlowNodeId, load: F) -> Result<&mut S>
	where
		S: 'static + Send,
		F: FnOnce(&mut Self) -> Result<(S, PersistFn)>,
	{
		if !self.inner().operator_states.contains_key(&node) {
			let (state, persist) = load(self)?;
			let slot = OperatorStateSlot {
				value: Box::new(state),
				dirty: false,
				persist,
			};
			self.inner_mut().operator_states.insert(node, slot);
		}
		let slot = self.inner_mut().operator_states.get_mut(&node).expect("just inserted");
		Ok(slot.value.downcast_mut::<S>().expect("operator state type mismatch"))
	}

	pub fn mark_state_dirty(&mut self, node: FlowNodeId) {
		if let Some(slot) = self.inner_mut().operator_states.get_mut(&node) {
			slot.dirty = true;
		}
	}

	pub fn take_operator_state<S, F>(&mut self, node: FlowNodeId, load: F) -> Result<(S, PersistFn)>
	where
		S: 'static + Send,
		F: FnOnce(&mut Self) -> Result<(S, PersistFn)>,
	{
		if let Some(slot) = self.inner_mut().operator_states.remove(&node) {
			let value = slot.value.downcast::<S>().map_err(|_| ()).expect("operator state type mismatch");
			Ok((*value, slot.persist))
		} else {
			load(self)
		}
	}

	pub fn put_operator_state<S>(&mut self, node: FlowNodeId, state: S, persist: PersistFn)
	where
		S: 'static + Send,
	{
		self.inner_mut().operator_states.insert(
			node,
			OperatorStateSlot {
				value: Box::new(state),
				dirty: true,
				persist,
			},
		);
	}

	#[instrument(name = "flow::actor::flush_state", level = "debug", skip_all)]
	pub fn flush_operator_states(&mut self) -> Result<()> {
		let states = mem::take(&mut self.inner_mut().operator_states);
		for (_, slot) in states {
			if slot.dirty {
				(slot.persist)(self, slot.value)?;
			}
		}
		Ok(())
	}

	pub fn install_operator_states(&mut self, states: HashMap<FlowNodeId, Box<dyn Any + Send>>) {
		let inner = self.inner_mut();
		for (node, value) in states {
			inner.operator_states.entry(node).or_insert_with(|| OperatorStateSlot {
				value,
				dirty: false,
				persist: Box::new(|_, _| Ok(())),
			});
		}
	}

	pub fn drain_operator_states(&mut self) -> HashMap<FlowNodeId, Box<dyn Any + Send>> {
		mem::take(&mut self.inner_mut().operator_states)
			.into_iter()
			.map(|(node, slot)| (node, slot.value))
			.collect()
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
	interceptor_method!(
		identity_attribute_post_create_interceptors,
		identity_attribute_post_create,
		IdentityAttributePostCreateInterceptor
	);
	interceptor_method!(
		identity_attribute_pre_delete_interceptors,
		identity_attribute_pre_delete,
		IdentityAttributePreDeleteInterceptor
	);
	interceptor_method!(
		identity_attribute_value_post_create_interceptors,
		identity_attribute_value_post_create,
		IdentityAttributeValuePostCreateInterceptor
	);
	interceptor_method!(
		identity_attribute_value_pre_delete_interceptors,
		identity_attribute_value_pre_delete,
		IdentityAttributeValuePreDeleteInterceptor
	);
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
