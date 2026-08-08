// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem, sync::Arc};

use read::ReadFrom;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, shape::RowShape},
};
use reifydb_core::{
	actors::pending::{Pending, PendingLayers, PendingWrite},
	common::CommitVersion,
	interface::{
		catalog::{flow::OperatorId, object::ObjectId},
		change::{Change, ChangeOrigin, Diff},
	},
	state::budget::OperatorStateBudgetHandle,
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_operator::store::OperatorStore;
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
		identity::{IdentityPostCreateInterceptor, IdentityPreDeleteInterceptor},
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
		role::{RolePostCreateInterceptor, RolePreDeleteInterceptor},
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
	transaction::admin::AdminTransaction,
};
use reifydb_value::{Result, value::datetime::DateTime};
use tracing::instrument;

pub mod dictionary;
pub mod frontier;
pub mod group;
pub mod read;
pub mod reclaim;
pub mod row_number;
pub mod slot;
pub mod state;
pub mod substrate;
pub mod timer;
pub mod watermark;
pub mod write;

use group::GroupInterner;
use row_number::RowNumberProvider;
use slot::{CarriedOperatorState, OperatorStateSlot, PersistFn, UsageFn};
use substrate::FlowSubstrate;
use timer::TimerWheel;
use watermark::SourceWatermarks;

use crate::{
	host::{HostRowShape, StandardHostRowShape},
	timer::Timer,
};

#[derive(Clone, Copy)]
pub struct ChangeCoordinate {
	pub at: Option<DateTime>,
	pub version: CommitVersion,
}

pub struct DeferredParams {
	pub version: CommitVersion,
	pub pending: Pending,
	pub base_pending: PendingLayers,
	pub query: MultiReadTransaction,
	pub state_query: MultiReadTransaction,
	pub single: SingleTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub clock: Clock,

	pub substrate: FlowSubstrate,

	pub state_budget: OperatorStateBudgetHandle,
}

pub struct FlowTransactionInner {
	pub version: CommitVersion,
	pub pending: Pending,
	pub base_pending: PendingLayers,
	pub pending_shapes: Vec<RowShape>,
	pub query: MultiReadTransaction,
	pub state_query: Option<MultiReadTransaction>,
	pub single: SingleTransaction,
	pub catalog: Catalog,
	pub host_row_shape: Arc<dyn HostRowShape>,
	pub interceptors: Interceptors,
	pub accumulator: ChangeAccumulator,
	pub clock: Clock,

	pub operator_states: HashMap<OperatorId, OperatorStateSlot>,

	pub prefetch: HashMap<EncodedKey, Option<EncodedBytes>>,
	pub prefetch_bytes: u64,
	pub prefetch_rejections: u64,

	pub store_reads: u64,

	pub change_coordinate: Option<ChangeCoordinate>,

	pub flow_watermark: Option<DateTime>,

	pub substrate: FlowSubstrate,

	pub state_budget: OperatorStateBudgetHandle,
}

impl Drop for FlowTransactionInner {
	fn drop(&mut self) {
		for slot in self.operator_states.values() {
			self.state_budget.release_dirty(slot.charged);
		}
	}
}

pub enum FlowTransaction {
	Deferred {
		inner: FlowTransactionInner,
	},

	Ephemeral {
		inner: FlowTransactionInner,

		state: HashMap<EncodedKey, EncodedBytes>,
	},
}

impl FlowTransaction {
	fn inner(&self) -> &FlowTransactionInner {
		match self {
			Self::Deferred {
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
			| Self::Ephemeral {
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
		Self::Deferred {
			inner: FlowTransactionInner {
				version,
				pending: Pending::new(),
				base_pending: PendingLayers::empty(),
				pending_shapes: Vec::new(),
				query,
				state_query: Some(state_query),
				single: parent.single.clone(),
				catalog: catalog.clone(),
				host_row_shape: Arc::new(StandardHostRowShape::new(catalog)),
				interceptors,
				accumulator: ChangeAccumulator::new(),
				clock,
				operator_states: HashMap::new(),
				prefetch: HashMap::new(),
				prefetch_bytes: 0,
				prefetch_rejections: 0,
				store_reads: 0,
				change_coordinate: None,
				flow_watermark: None,
				substrate: FlowSubstrate::new(),
				state_budget: OperatorStateBudgetHandle::default(),
			},
		}
	}

	pub fn deferred_from_parts(params: DeferredParams) -> Self {
		let mut query = params.query;
		query.read_as_of_version_inclusive(params.version);
		let state_query = params.state_query;

		Self::Deferred {
			inner: FlowTransactionInner {
				version: params.version,
				pending: params.pending,
				base_pending: params.base_pending,
				pending_shapes: Vec::new(),
				query,
				state_query: Some(state_query),
				single: params.single,
				catalog: params.catalog.clone(),
				host_row_shape: Arc::new(StandardHostRowShape::new(params.catalog)),
				interceptors: params.interceptors,
				accumulator: ChangeAccumulator::new(),
				clock: params.clock,
				operator_states: HashMap::new(),
				prefetch: HashMap::new(),
				prefetch_bytes: 0,
				prefetch_rejections: 0,
				store_reads: 0,
				change_coordinate: None,
				flow_watermark: None,
				substrate: params.substrate,
				state_budget: params.state_budget,
			},
		}
	}

	pub fn row_numbers(&self) -> RowNumberProvider {
		self.inner().substrate.row.clone()
	}

	pub fn group_interner(&self) -> GroupInterner {
		self.inner().substrate.group.clone()
	}

	pub fn dictionary_allocators(&self) -> DictionaryAllocatorRegistry {
		self.inner().substrate.dictionary.clone()
	}

	pub fn source_watermarks(&self) -> SourceWatermarks {
		self.inner().substrate.watermarks.clone()
	}

	pub fn timer_wheel(&self) -> TimerWheel {
		self.inner().substrate.timers.clone()
	}

	pub fn operator_store(&self) -> OperatorStore {
		self.inner().substrate.operators.clone()
	}

	pub fn arm_timer(&mut self, operator: OperatorId, timer: &Timer) -> Result<()> {
		self.timer_wheel().arm(operator, self, timer)
	}

	pub fn disarm_timer(&mut self, operator: OperatorId, timer: &Timer) -> Result<()> {
		self.timer_wheel().disarm(operator, self, timer)
	}

	pub fn set_change_coordinate(&mut self, coordinate: ChangeCoordinate) {
		self.inner_mut().change_coordinate = Some(coordinate);
	}

	pub(crate) fn change_coordinate(&self) -> Option<ChangeCoordinate> {
		self.inner().change_coordinate
	}

	/// The event time a write made now should carry. Operator state ages against the same clock the
	/// group buckets are derived from, so a row stamped from the wall clock would expire on a
	/// different timeline during a replay.
	pub fn written_at(&self) -> DateTime {
		match self.change_coordinate().and_then(|coordinate| coordinate.at) {
			Some(at) => at,
			None => self.clock().now(),
		}
	}

	pub fn set_flow_watermark(&mut self, watermark: DateTime) {
		self.inner_mut().flow_watermark = Some(watermark);
	}

	pub fn flow_watermark(&self) -> Option<DateTime> {
		self.inner().flow_watermark
	}

	pub fn ephemeral(
		version: CommitVersion,
		query: MultiReadTransaction,
		single: SingleTransaction,
		catalog: Catalog,
		state: HashMap<EncodedKey, EncodedBytes>,
		clock: Clock,
		state_budget: OperatorStateBudgetHandle,
	) -> Self {
		let mut pq = query;
		pq.read_as_of_version_inclusive(version);

		Self::Ephemeral {
			inner: FlowTransactionInner {
				version,
				pending: Pending::new(),
				base_pending: PendingLayers::empty(),
				pending_shapes: Vec::new(),
				query: pq,
				state_query: None,
				single,
				catalog: catalog.clone(),
				host_row_shape: Arc::new(StandardHostRowShape::new(catalog)),
				interceptors: Interceptors::new(),
				accumulator: ChangeAccumulator::new(),
				clock,
				operator_states: HashMap::new(),
				prefetch: HashMap::new(),
				prefetch_bytes: 0,
				prefetch_rejections: 0,
				store_reads: 0,
				change_coordinate: None,
				flow_watermark: None,
				substrate: FlowSubstrate::new(),
				state_budget,
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
				if matches!(Self::read_from(key), ReadFrom::OperatorState | ReadFrom::StateQuery) {
					match write {
						PendingWrite::Set(row) => {
							state.insert(key.clone(), row.clone());
						}
						PendingWrite::Remove {
							..
						} => {
							state.remove(key);
						}
					}
				}
			}
			inner.pending = Pending::new();
		}
	}

	pub fn take_state(&mut self) -> HashMap<EncodedKey, EncodedBytes> {
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

	pub fn store_reads(&self) -> u64 {
		self.inner().store_reads
	}

	pub fn take_pending(&mut self) -> Pending {
		mem::take(&mut self.inner_mut().pending)
	}

	pub fn take_pending_shapes(&mut self) -> Vec<RowShape> {
		mem::take(&mut self.inner_mut().pending_shapes)
	}

	pub fn track_flow_change(&mut self, change: Change) {
		if let ChangeOrigin::Object(id) = change.origin {
			for diff in change.diffs {
				self.inner_mut().accumulator.track(id, diff);
			}
		}
	}

	pub fn take_accumulator_entries(&mut self) -> Vec<(ObjectId, Diff)> {
		let acc = &mut self.inner_mut().accumulator;
		let entries: Vec<_> = acc.entries_from(0).to_vec();
		acc.clear();
		entries
	}

	pub fn pending(&self) -> &Pending {
		&self.inner().pending
	}

	pub fn catalog(&self) -> &Catalog {
		&self.inner().catalog
	}

	pub fn query_and_single(&self) -> (MultiReadTransaction, SingleTransaction) {
		let inner = self.inner();
		(inner.query.clone(), inner.single.clone())
	}

	pub fn host_row_shape(&self) -> &dyn HostRowShape {
		&*self.inner().host_row_shape
	}

	pub fn clock(&self) -> &Clock {
		&self.inner().clock
	}

	pub fn state_budget(&self) -> OperatorStateBudgetHandle {
		self.inner().state_budget.clone()
	}

	pub fn operator_state<S, F>(&mut self, operator: OperatorId, usage: UsageFn, load: F) -> Result<&mut S>
	where
		S: 'static + Send,
		F: FnOnce(&mut Self) -> Result<(S, PersistFn)>,
	{
		if !self.inner().operator_states.contains_key(&operator) {
			let (state, persist) = load(self)?;
			let charged = usage(&state);
			let inner = self.inner_mut();
			inner.state_budget.charge_dirty(charged);
			let slot = OperatorStateSlot {
				value: Box::new(state),
				dirty: false,
				persist,
				usage,
				charged,
			};
			inner.operator_states.insert(operator, slot);
		}
		let slot = self.inner_mut().operator_states.get_mut(&operator).expect("just inserted");
		Ok(slot.value.downcast_mut::<S>().expect("operator state type mismatch"))
	}

	pub fn mark_state_dirty(&mut self, operator: OperatorId) {
		if let Some(slot) = self.inner_mut().operator_states.get_mut(&operator) {
			slot.dirty = true;
		}
	}

	pub fn take_operator_state<S, F>(&mut self, operator: OperatorId, load: F) -> Result<(S, PersistFn)>
	where
		S: 'static + Send,
		F: FnOnce(&mut Self) -> Result<(S, PersistFn)>,
	{
		if let Some(slot) = self.inner_mut().operator_states.remove(&operator) {
			self.inner().state_budget.release_dirty(slot.charged);
			let value = slot.value.downcast::<S>().map_err(|_| ()).expect("operator state type mismatch");
			Ok((*value, slot.persist))
		} else {
			load(self)
		}
	}

	pub fn put_operator_state<S>(&mut self, operator: OperatorId, state: S, persist: PersistFn, usage: UsageFn)
	where
		S: 'static + Send,
	{
		let charged = usage(&state);
		let inner = self.inner_mut();
		inner.state_budget.charge_dirty(charged);
		let replaced = inner.operator_states.insert(
			operator,
			OperatorStateSlot {
				value: Box::new(state),
				dirty: true,
				persist,
				usage,
				charged,
			},
		);
		if let Some(replaced) = replaced {
			inner.state_budget.release_dirty(replaced.charged);
		}
	}

	#[instrument(name = "flow::actor::flush_state", level = "debug", skip_all)]
	pub fn flush_operator_states(&mut self) -> Result<()> {
		let states = mem::take(&mut self.inner_mut().operator_states);
		let budget = self.inner().state_budget.clone();
		for (_, slot) in states {
			let current = (slot.usage)(&*slot.value);
			budget.release_dirty(slot.charged);
			budget.charge_dirty(current);
			let outcome = if slot.dirty {
				(slot.persist)(self, slot.value)
			} else {
				Ok(())
			};
			budget.release_dirty(current);
			outcome?;
		}
		Ok(())
	}

	pub fn install_operator_states(&mut self, states: HashMap<OperatorId, CarriedOperatorState>) {
		let inner = self.inner_mut();
		for (operator, carried) in states {
			if inner.operator_states.contains_key(&operator) {
				continue;
			}
			let charged = (carried.usage)(&*carried.value);
			inner.state_budget.charge_dirty(charged);
			inner.operator_states.insert(
				operator,
				OperatorStateSlot {
					value: carried.value,
					dirty: false,
					persist: Box::new(|_, _| Ok(())),
					usage: carried.usage,
					charged,
				},
			);
		}
	}

	pub fn drain_operator_states(&mut self) -> HashMap<OperatorId, CarriedOperatorState> {
		let inner = self.inner_mut();
		mem::take(&mut inner.operator_states)
			.into_iter()
			.map(|(operator, slot)| {
				inner.state_budget.release_dirty(slot.charged);
				(
					operator,
					CarriedOperatorState {
						value: slot.value,
						usage: slot.usage,
					},
				)
			})
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
