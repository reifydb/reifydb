// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem};

use read::ReadFrom;
use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::{
	actors::pending::{Pending, PendingLayers, PendingWrite},
	common::CommitVersion,
	interface::{
		catalog::{flow::OperatorId, object::ObjectId},
		change::{Change, ChangeOrigin, Diff},
		store::MultiVersionRow,
	},
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
	multi::{RangeScope, transaction::read::MultiReadTransaction},
	transaction::admin::AdminTransaction,
};
use reifydb_value::{Result, value::datetime::DateTime};
use tracing::instrument;

pub mod deferred;
pub mod dictionary;
pub mod ephemeral;
pub mod frontier;
pub mod group;
pub mod interface;
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
use slot::{CarriedOperatorState, OperatorStateSlot, PersistFn};
use substrate::FlowSubstrate;
use timer::TimerWheel;
use watermark::SourceWatermarks;

use crate::{
	timer::Timer,
	transaction::{
		deferred::{
			deferred_fetch_state_external, deferred_storage_contains, deferred_storage_get,
			deferred_storage_range, deferred_storage_range_rev,
		},
		ephemeral::{
			ephemeral_fetch_state_external, ephemeral_storage_contains, ephemeral_storage_get,
			ephemeral_storage_range, ephemeral_storage_range_rev,
		},
		interface::FlowTransaction,
	},
};

#[derive(Clone, Copy)]
pub struct ChangeCoordinate {
	pub at: Option<DateTime>,
	pub version: CommitVersion,
}

pub struct DeferredParams {
	pub version: CommitVersion,
	pub pending: PendingLayers,
	pub query: MultiReadTransaction,
	pub state_query: MultiReadTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub clock: Clock,

	pub substrate: FlowSubstrate,
}

pub struct FlowTransactionDeferred {
	pub version: CommitVersion,
	pub pending: PendingLayers,
	pub query: MultiReadTransaction,
	pub state_query: MultiReadTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub accumulator: ChangeAccumulator,
	pub clock: Clock,

	pub operator_states: HashMap<OperatorId, OperatorStateSlot>,

	pub change_coordinate: Option<ChangeCoordinate>,

	pub flow_watermark: Option<DateTime>,

	pub substrate: FlowSubstrate,
}

pub struct FlowTransactionEphemeral {
	pub version: CommitVersion,
	pub pending: PendingLayers,
	pub query: MultiReadTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub accumulator: ChangeAccumulator,
	pub clock: Clock,

	pub operator_states: HashMap<OperatorId, OperatorStateSlot>,

	pub change_coordinate: Option<ChangeCoordinate>,

	pub flow_watermark: Option<DateTime>,

	pub substrate: FlowSubstrate,

	pub state: HashMap<EncodedKey, EncodedBytes>,
}

pub enum DepFlowTransaction {
	Deferred(FlowTransactionDeferred),

	Ephemeral(FlowTransactionEphemeral),
}

impl DepFlowTransaction {
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
		Self::Deferred(FlowTransactionDeferred {
			version,
			pending: PendingLayers::empty(),
			query,
			state_query,
			catalog: catalog.clone(),
			interceptors,
			accumulator: ChangeAccumulator::new(),
			clock,
			operator_states: HashMap::new(),
			change_coordinate: None,
			flow_watermark: None,
			substrate: FlowSubstrate::new(),
		})
	}

	pub fn deferred_from_parts(params: DeferredParams) -> Self {
		let mut query = params.query;
		query.read_as_of_version_inclusive(params.version);
		let state_query = params.state_query;

		Self::Deferred(FlowTransactionDeferred {
			version: params.version,
			pending: params.pending,
			query,
			state_query,
			catalog: params.catalog.clone(),
			interceptors: params.interceptors,
			accumulator: ChangeAccumulator::new(),
			clock: params.clock,
			operator_states: HashMap::new(),
			change_coordinate: None,
			flow_watermark: None,
			substrate: params.substrate,
		})
	}

	pub fn row_numbers(&self) -> RowNumberProvider {
		match self {
			Self::Deferred(d) => d.substrate.row.clone(),
			Self::Ephemeral(e) => e.substrate.row.clone(),
		}
	}

	pub fn group_interner(&self) -> GroupInterner {
		match self {
			Self::Deferred(d) => d.substrate.group.clone(),
			Self::Ephemeral(e) => e.substrate.group.clone(),
		}
	}

	pub fn dictionary_allocators(&self) -> DictionaryAllocatorRegistry {
		match self {
			Self::Deferred(d) => d.substrate.dictionary.clone(),
			Self::Ephemeral(e) => e.substrate.dictionary.clone(),
		}
	}

	pub fn source_watermarks(&self) -> SourceWatermarks {
		match self {
			Self::Deferred(d) => d.substrate.watermarks.clone(),
			Self::Ephemeral(e) => e.substrate.watermarks.clone(),
		}
	}

	pub fn timer_wheel(&self) -> TimerWheel {
		match self {
			Self::Deferred(d) => d.substrate.timers.clone(),
			Self::Ephemeral(e) => e.substrate.timers.clone(),
		}
	}

	pub fn operator_store(&self) -> OperatorStore {
		match self {
			Self::Deferred(d) => d.substrate.operators.clone(),
			Self::Ephemeral(e) => e.substrate.operators.clone(),
		}
	}

	pub fn arm_timer(&mut self, operator: OperatorId, timer: &Timer) -> Result<()> {
		self.timer_wheel().arm(operator, self, timer)
	}

	pub fn disarm_timer(&mut self, operator: OperatorId, timer: &Timer) -> Result<()> {
		self.timer_wheel().disarm(operator, self, timer)
	}

	pub fn set_change_coordinate(&mut self, coordinate: ChangeCoordinate) {
		match self {
			Self::Deferred(d) => d.change_coordinate = Some(coordinate),
			Self::Ephemeral(e) => e.change_coordinate = Some(coordinate),
		}
	}

	pub(crate) fn change_coordinate(&self) -> Option<ChangeCoordinate> {
		match self {
			Self::Deferred(d) => d.change_coordinate,
			Self::Ephemeral(e) => e.change_coordinate,
		}
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
		match self {
			Self::Deferred(d) => d.flow_watermark = Some(watermark),
			Self::Ephemeral(e) => e.flow_watermark = Some(watermark),
		}
	}

	pub fn flow_watermark(&self) -> Option<DateTime> {
		match self {
			Self::Deferred(d) => d.flow_watermark,
			Self::Ephemeral(e) => e.flow_watermark,
		}
	}

	pub fn ephemeral(
		version: CommitVersion,
		query: MultiReadTransaction,
		catalog: Catalog,
		state: HashMap<EncodedKey, EncodedBytes>,
		clock: Clock,
	) -> Self {
		let mut pq = query;
		pq.read_as_of_version_inclusive(version);

		Self::Ephemeral(FlowTransactionEphemeral {
			version,
			pending: PendingLayers::empty(),
			query: pq,
			catalog: catalog.clone(),
			interceptors: Interceptors::new(),
			accumulator: ChangeAccumulator::new(),
			clock,
			operator_states: HashMap::new(),
			change_coordinate: None,
			flow_watermark: None,
			substrate: FlowSubstrate::new(),
			state,
		})
	}

	pub fn merge_state(&mut self) {
		if let Self::Ephemeral(e) = self {
			let own = e.pending.take_top();
			for (key, write) in own.iter_sorted() {
				if matches!(Self::read_from(key), ReadFrom::OperatorState | ReadFrom::StateQuery) {
					match write {
						PendingWrite::Set(row) => {
							e.state.insert(key.clone(), row.clone());
						}
						PendingWrite::Remove {
							..
						} => {
							e.state.remove(key);
						}
					}
				}
			}
		}
	}

	pub fn take_state(&mut self) -> HashMap<EncodedKey, EncodedBytes> {
		if let Self::Ephemeral(e) = self {
			mem::take(&mut e.state)
		} else {
			HashMap::new()
		}
	}

	pub fn version(&self) -> CommitVersion {
		match self {
			Self::Deferred(d) => d.version,
			Self::Ephemeral(e) => e.version,
		}
	}

	pub fn take_pending(&mut self) -> Pending {
		match self {
			Self::Deferred(d) => d.pending.take_top(),
			Self::Ephemeral(e) => e.pending.take_top(),
		}
	}

	pub fn track_flow_change(&mut self, change: Change) {
		if let ChangeOrigin::Object(id) = change.origin {
			let accumulator = match self {
				Self::Deferred(d) => &mut d.accumulator,
				Self::Ephemeral(e) => &mut e.accumulator,
			};
			for diff in change.diffs {
				accumulator.track(id, diff);
			}
		}
	}

	pub fn take_accumulator_entries(&mut self) -> Vec<(ObjectId, Diff)> {
		let acc = match self {
			Self::Deferred(d) => &mut d.accumulator,
			Self::Ephemeral(e) => &mut e.accumulator,
		};
		let entries: Vec<_> = acc.entries_from(0).to_vec();
		acc.clear();
		entries
	}

	pub fn pending(&self) -> &Pending {
		match self {
			Self::Deferred(d) => d.pending.top(),
			Self::Ephemeral(e) => e.pending.top(),
		}
	}

	pub fn catalog(&self) -> &Catalog {
		match self {
			Self::Deferred(d) => &d.catalog,
			Self::Ephemeral(e) => &e.catalog,
		}
	}

	pub fn query(&self) -> MultiReadTransaction {
		match self {
			Self::Deferred(d) => d.query.clone(),
			Self::Ephemeral(e) => e.query.clone(),
		}
	}

	pub fn clock(&self) -> &Clock {
		match self {
			Self::Deferred(d) => &d.clock,
			Self::Ephemeral(e) => &e.clock,
		}
	}

	fn operator_states_mut(&mut self) -> &mut HashMap<OperatorId, OperatorStateSlot> {
		match self {
			Self::Deferred(d) => &mut d.operator_states,
			Self::Ephemeral(e) => &mut e.operator_states,
		}
	}

	pub fn operator_state<S, F>(&mut self, operator: OperatorId, load: F) -> Result<&mut S>
	where
		S: 'static + Send,
		F: FnOnce(&mut Self) -> Result<(S, PersistFn)>,
	{
		if !self.operator_states_mut().contains_key(&operator) {
			let (state, persist) = load(self)?;
			let slot = OperatorStateSlot {
				value: Box::new(state),
				dirty: false,
				persist,
			};
			self.operator_states_mut().insert(operator, slot);
		}
		let slot = self.operator_states_mut().get_mut(&operator).expect("just inserted");
		Ok(slot.value.downcast_mut::<S>().expect("operator state type mismatch"))
	}

	pub fn mark_state_dirty(&mut self, operator: OperatorId) {
		if let Some(slot) = self.operator_states_mut().get_mut(&operator) {
			slot.dirty = true;
		}
	}

	pub fn take_operator_state<S, F>(&mut self, operator: OperatorId, load: F) -> Result<(S, PersistFn)>
	where
		S: 'static + Send,
		F: FnOnce(&mut Self) -> Result<(S, PersistFn)>,
	{
		if let Some(slot) = self.operator_states_mut().remove(&operator) {
			let value = slot.value.downcast::<S>().map_err(|_| ()).expect("operator state type mismatch");
			Ok((*value, slot.persist))
		} else {
			load(self)
		}
	}

	pub fn put_operator_state<S>(&mut self, operator: OperatorId, state: S, persist: PersistFn)
	where
		S: 'static + Send,
	{
		self.operator_states_mut().insert(
			operator,
			OperatorStateSlot {
				value: Box::new(state),
				dirty: true,
				persist,
			},
		);
	}

	#[instrument(name = "flow::actor::flush_state", level = "debug", skip_all)]
	pub fn flush_operator_states(&mut self) -> Result<()> {
		let states = mem::take(self.operator_states_mut());
		for (_, slot) in states {
			if slot.dirty {
				(slot.persist)(self, slot.value)?;
			}
		}
		Ok(())
	}

	pub fn install_operator_states(&mut self, states: HashMap<OperatorId, CarriedOperatorState>) {
		for (operator, carried) in states {
			if self.operator_states_mut().contains_key(&operator) {
				continue;
			}
			self.operator_states_mut().insert(
				operator,
				OperatorStateSlot {
					value: carried.value,
					dirty: false,
					persist: Box::new(|_, _| Ok(())),
				},
			);
		}
	}

	pub fn drain_operator_states(&mut self) -> HashMap<OperatorId, CarriedOperatorState> {
		mem::take(self.operator_states_mut())
			.into_iter()
			.map(|(operator, slot)| {
				(
					operator,
					CarriedOperatorState {
						value: slot.value,
					},
				)
			})
			.collect()
	}
}

macro_rules! interceptor_method {
	($method:ident, $field:ident, $trait_name:ident) => {
		fn $method(&mut self) -> &mut Chain<dyn $trait_name + Send + Sync> {
			match self {
				Self::Deferred(d) => &mut d.interceptors.$field,
				Self::Ephemeral(e) => &mut e.interceptors.$field,
			}
		}
	};
}

impl WithInterceptors for DepFlowTransaction {
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

impl FlowTransaction for DepFlowTransaction {
	fn version(&self) -> CommitVersion {
		match self {
			Self::Deferred(d) => d.version,
			Self::Ephemeral(e) => e.version,
		}
	}

	fn clock(&self) -> &Clock {
		match self {
			Self::Deferred(d) => &d.clock,
			Self::Ephemeral(e) => &e.clock,
		}
	}

	fn catalog(&self) -> &Catalog {
		match self {
			Self::Deferred(d) => &d.catalog,
			Self::Ephemeral(e) => &e.catalog,
		}
	}

	fn query(&self) -> MultiReadTransaction {
		match self {
			Self::Deferred(d) => d.query.clone(),
			Self::Ephemeral(e) => e.query.clone(),
		}
	}

	fn substrate(&self) -> &FlowSubstrate {
		match self {
			Self::Deferred(d) => &d.substrate,
			Self::Ephemeral(e) => &e.substrate,
		}
	}

	fn pending_layers(&self) -> &PendingLayers {
		match self {
			Self::Deferred(d) => &d.pending,
			Self::Ephemeral(e) => &e.pending,
		}
	}

	fn pending_layers_mut(&mut self) -> &mut PendingLayers {
		match self {
			Self::Deferred(d) => &mut d.pending,
			Self::Ephemeral(e) => &mut e.pending,
		}
	}

	fn accumulator_mut(&mut self) -> &mut ChangeAccumulator {
		match self {
			Self::Deferred(d) => &mut d.accumulator,
			Self::Ephemeral(e) => &mut e.accumulator,
		}
	}

	fn operator_states_mut(&mut self) -> &mut HashMap<OperatorId, OperatorStateSlot<Self>> {
		match self {
			Self::Deferred(d) => &mut d.operator_states,
			Self::Ephemeral(e) => &mut e.operator_states,
		}
	}

	fn change_coordinate(&self) -> Option<ChangeCoordinate> {
		match self {
			Self::Deferred(d) => d.change_coordinate,
			Self::Ephemeral(e) => e.change_coordinate,
		}
	}

	fn set_change_coordinate(&mut self, coordinate: ChangeCoordinate) {
		match self {
			Self::Deferred(d) => d.change_coordinate = Some(coordinate),
			Self::Ephemeral(e) => e.change_coordinate = Some(coordinate),
		}
	}

	fn flow_watermark(&self) -> Option<DateTime> {
		match self {
			Self::Deferred(d) => d.flow_watermark,
			Self::Ephemeral(e) => e.flow_watermark,
		}
	}

	fn set_flow_watermark(&mut self, watermark: DateTime) {
		match self {
			Self::Deferred(d) => d.flow_watermark = Some(watermark),
			Self::Ephemeral(e) => e.flow_watermark = Some(watermark),
		}
	}

	fn storage_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		match self {
			Self::Deferred(d) => {
				deferred_storage_get(&d.substrate.operators, &d.query, &d.state_query, key)
			}
			Self::Ephemeral(e) => ephemeral_storage_get(&e.state, &e.query, key),
		}
	}

	fn storage_contains(&mut self, key: &EncodedKey) -> Result<bool> {
		match self {
			Self::Deferred(d) => {
				deferred_storage_contains(&d.substrate.operators, &d.query, &d.state_query, key)
			}
			Self::Ephemeral(e) => ephemeral_storage_contains(&e.state, &e.query, key),
		}
	}

	fn storage_range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		match self {
			Self::Deferred(d) => deferred_storage_range(
				&d.substrate.operators,
				&d.query,
				&d.state_query,
				d.version,
				range,
				scope,
				batch_size,
			),
			Self::Ephemeral(e) => {
				ephemeral_storage_range(&e.state, &e.query, e.version, range, scope, batch_size)
			}
		}
	}

	fn storage_range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		match self {
			Self::Deferred(d) => deferred_storage_range_rev(
				&d.substrate.operators,
				&d.query,
				&d.state_query,
				d.version,
				range,
				scope,
				batch_size,
			),
			Self::Ephemeral(e) => {
				ephemeral_storage_range_rev(&e.state, &e.query, e.version, range, scope, batch_size)
			}
		}
	}

	fn fetch_state_external(&mut self, keys: &[EncodedKey], items: &mut Vec<MultiVersionRow>) -> Result<()> {
		match self {
			Self::Deferred(d) => {
				deferred_fetch_state_external(&d.substrate.operators, d.version, keys, items)
			}
			Self::Ephemeral(e) => ephemeral_fetch_state_external(&e.state, e.version, keys, items),
		}
		Ok(())
	}
}
