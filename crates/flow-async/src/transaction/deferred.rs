// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	ops::Bound::{Excluded, Included, Unbounded},
};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, pod::EncodedPodRow, shape::RowShape},
};
use reifydb_core::{
	actors::pending::Pending,
	common::{CommitVersion, SourceVersion},
	interface::{
		catalog::{dictionary::Dictionary, flow::OperatorId, id::ViewId},
		change::{Change, Diff},
		store::MultiVersionRow,
	},
	key::{any::TaggedKey, operator::state::GroupStateKey},
};
use reifydb_flow::txn::{Batch, Emit, Get, Intern, Resolve};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_operator::store::OperatorStore;
use reifydb_transaction::{
	interceptor::{
		WithInterceptors,
		authentication::{AuthenticationPostCreateInterceptor, AuthenticationPreDeleteInterceptor},
		chain::InterceptorChain as Chain,
		dictionary::{
			DictionaryPostCreateInterceptor, DictionaryPostUpdateInterceptor,
			DictionaryPreDeleteInterceptor, DictionaryPreUpdateInterceptor,
		},
		dictionary_row::{
			DictionaryRowInterceptor, DictionaryRowPostDeleteInterceptor,
			DictionaryRowPostInsertInterceptor, DictionaryRowPostUpdateInterceptor,
			DictionaryRowPreDeleteInterceptor, DictionaryRowPreInsertInterceptor,
			DictionaryRowPreUpdateInterceptor,
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
		transaction::PostCommitInterceptor,
		view::{
			ViewPostCreateInterceptor, ViewPostUpdateInterceptor, ViewPreDeleteInterceptor,
			ViewPreUpdateInterceptor,
		},
	},
	multi::{RangeScope, transaction::read::MultiReadTransaction},
};
use reifydb_value::{
	Result,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};
use tracing::instrument;

use crate::{
	operator::sink::DurableSink,
	timer::{Timer, TimerDue},
	transaction::{
		ChangeCoordinate, DeferredParams, FlowTransaction, LookupContext,
		dictionary::DictionaryExtension,
		pending_get, pending_get_many,
		read::{OperatorStateRangeIter, ReadFrom, read_from},
		scope::{OperatorRangeScope, OperatorScope, operator_state_coordinates, operator_state_scope},
		substrate::FlowSubstrate,
	},
};

pub struct DeferredTransaction {
	pub version: CommitVersion,
	pub pending: Pending,
	pub query: Option<MultiReadTransaction>,
	pub state_query: Option<MultiReadTransaction>,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub emitted: bool,
	pub armed: Vec<TimerDue>,
	pub clock: Clock,

	pub change_coordinate: Option<ChangeCoordinate>,

	pub flow_watermark: Option<DateTime>,

	pub source_watermark_cache: HashMap<OperatorId, i64>,

	pub row_shape_cache: HashMap<OperatorId, HashMap<EncodedKey, RowShape>>,

	pub substrate: FlowSubstrate,

	pub lookup: Option<LookupContext>,

	#[cfg(test)]
	pub storage_counts: StorageCounts,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageCounts {
	pub gets: usize,
	pub get_many_calls: usize,
	pub state_fetch_calls: usize,
	pub ranges: usize,
}

impl DeferredTransaction {
	#[instrument(name = "flow::transaction::deferred", level = "debug", skip(params), fields(version = params.version.0))]
	pub fn new(params: DeferredParams) -> Self {
		let mut query = params.query;
		if let Some(query) = query.as_mut() {
			query.read_as_of_version_inclusive(params.version);
		}

		Self {
			version: params.version,
			pending: params.pending,
			query,
			state_query: params.state_query,
			catalog: params.catalog,
			interceptors: params.interceptors,
			emitted: false,
			armed: Vec::new(),
			clock: params.clock,
			change_coordinate: None,
			flow_watermark: None,
			source_watermark_cache: HashMap::new(),
			row_shape_cache: HashMap::new(),
			substrate: params.substrate,
			lookup: params.lookup,
			#[cfg(test)]
			storage_counts: StorageCounts::default(),
		}
	}
}

const NO_OPERATOR_STORE: &str = "flow transaction was built without an operator store";
const NO_READ_TRANSACTION: &str = "flow transaction was built without a read transaction";
const UNDECODABLE_KEY: &str = "a key routed to the multi store must decode";

pub(crate) fn deferred_storage_get(
	operators: Option<&OperatorStore>,
	query: Option<&MultiReadTransaction>,
	state_query: Option<&MultiReadTransaction>,
	key: &EncodedKey,
) -> Result<Option<EncodedBytes>> {
	let route = read_from(key);
	if matches!(route, ReadFrom::OperatorState) {
		let OperatorScope {
			operator,
			inner,
		} = operator_state_coordinates(key).expect("an OperatorState-routed key must carry an operator id");
		return Ok(operators
			.expect(NO_OPERATOR_STORE)
			.state_get(operator, &GroupStateKey::bound_unchecked(inner))?
			.map(EncodedPodRow::into_bytes));
	}
	let query = match route {
		ReadFrom::StateQuery | ReadFrom::OwnedRow => state_query,
		ReadFrom::Query => query,
		ReadFrom::OperatorState => unreachable!(),
	};
	let key = TaggedKey::decode(key).expect(UNDECODABLE_KEY);
	Ok(query.expect(NO_READ_TRANSACTION).get(&key)?.map(|value| value.into_multi_version_row().bytes))
}

pub(crate) fn deferred_storage_get_many(
	operators: Option<&OperatorStore>,
	query: Option<&MultiReadTransaction>,
	state_query: Option<&MultiReadTransaction>,
	keys: &[EncodedKey],
) -> Result<Vec<Option<EncodedBytes>>> {
	let mut rows = vec![None; keys.len()];
	let (mut state_slots, mut state_keys) = (Vec::new(), Vec::new());
	let (mut query_slots, mut query_keys) = (Vec::new(), Vec::new());
	for (slot, key) in keys.iter().enumerate() {
		match read_from(key) {
			ReadFrom::OperatorState => {
				rows[slot] = deferred_storage_get(operators, query, state_query, key)?
			}
			ReadFrom::StateQuery | ReadFrom::OwnedRow => {
				state_slots.push(slot);
				state_keys.push(TaggedKey::decode(key).expect(UNDECODABLE_KEY));
			}
			ReadFrom::Query => {
				query_slots.push(slot);
				query_keys.push(TaggedKey::decode(key).expect(UNDECODABLE_KEY));
			}
		}
	}
	for (read, slots, tagged) in [(state_query, state_slots, state_keys), (query, query_slots, query_keys)] {
		if tagged.is_empty() {
			continue;
		}
		for (slot, value) in slots.into_iter().zip(read.expect(NO_READ_TRANSACTION).get_many(&tagged)?) {
			rows[slot] = value.map(|value| value.into_multi_version_row().bytes);
		}
	}
	Ok(rows)
}

pub(crate) fn deferred_storage_contains(
	operators: Option<&OperatorStore>,
	query: Option<&MultiReadTransaction>,
	state_query: Option<&MultiReadTransaction>,
	key: &EncodedKey,
) -> Result<bool> {
	let query = match read_from(key) {
		ReadFrom::OperatorState => {
			let OperatorScope {
				operator,
				inner,
			} = operator_state_coordinates(key)
				.expect("an OperatorState-routed key must carry an operator id");
			return operators
				.expect(NO_OPERATOR_STORE)
				.contains(operator, &GroupStateKey::bound_unchecked(inner))
				.map_err(Into::into);
		}
		ReadFrom::StateQuery | ReadFrom::OwnedRow => state_query,
		ReadFrom::Query => query,
	};
	query.expect(NO_READ_TRANSACTION).contains(&TaggedKey::decode(key).expect(UNDECODABLE_KEY))
}

pub(crate) fn deferred_storage_range<'a>(
	operators: Option<&OperatorStore>,
	query: Option<&'a MultiReadTransaction>,
	state_query: Option<&'a MultiReadTransaction>,
	version: CommitVersion,
	range: EncodedKeyRange,
	scope: RangeScope,
	batch_size: usize,
) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + 'a> {
	if let Some(OperatorRangeScope {
		operator,
		inner,
	}) = operator_state_scope(&range)
	{
		return Box::new(OperatorStateRangeIter::new(
			operators.expect(NO_OPERATOR_STORE).clone(),
			operator,
			inner,
			batch_size,
			version,
		));
	}
	let query = deferred_range_target(query, state_query, &range);
	Box::new(query.range_encoded(range, scope, batch_size))
}

fn deferred_range_target<'a>(
	query: Option<&'a MultiReadTransaction>,
	state_query: Option<&'a MultiReadTransaction>,
	range: &EncodedKeyRange,
) -> &'a MultiReadTransaction {
	match range.start.as_ref() {
		Included(start) | Excluded(start) => match read_from(start) {
			ReadFrom::OperatorState => {
				unreachable!("operator-state ranges take the operator-state path")
			}
			ReadFrom::StateQuery | ReadFrom::OwnedRow => state_query,
			ReadFrom::Query => query,
		},
		Unbounded => query,
	}
	.expect(NO_READ_TRANSACTION)
}

pub(crate) fn deferred_fetch_state(
	operators: Option<&OperatorStore>,
	id: OperatorId,
	keys: &[GroupStateKey],
) -> Result<Vec<Option<EncodedBytes>>> {
	if keys.is_empty() {
		return Ok(Vec::new());
	}
	Ok(operators
		.expect(NO_OPERATOR_STORE)
		.state_get_many(id, keys)?
		.into_iter()
		.map(|row| row.map(EncodedPodRow::into_bytes))
		.collect())
}

impl FlowTransaction for DeferredTransaction {
	fn version(&self) -> CommitVersion {
		self.version
	}

	fn lookup_floor(&self) -> Option<CommitVersion> {
		self.lookup.as_ref().map(|lookup| lookup.floor)
	}

	fn lookup_view_version(&self, view: ViewId, source: SourceVersion) -> Option<CommitVersion> {
		self.lookup.as_ref().and_then(|lookup| lookup.versions.view_version(view, source))
	}

	fn lookup_view_commit_through(&self, view: ViewId, commit: CommitVersion) -> Option<CommitVersion> {
		self.lookup.as_ref().and_then(|lookup| lookup.versions.view_commit_through(view, commit))
	}

	fn clock(&self) -> &Clock {
		&self.clock
	}

	fn catalog(&self) -> &Catalog {
		&self.catalog
	}

	fn query(&self) -> MultiReadTransaction {
		self.query.clone().expect(NO_READ_TRANSACTION)
	}

	fn substrate(&self) -> &FlowSubstrate {
		&self.substrate
	}

	fn pending(&self) -> &Pending {
		&self.pending
	}

	fn pending_mut(&mut self) -> &mut Pending {
		&mut self.pending
	}

	fn armed_mut(&mut self) -> &mut Vec<TimerDue> {
		&mut self.armed
	}

	fn change_coordinate(&self) -> Option<ChangeCoordinate> {
		self.change_coordinate
	}

	fn set_change_coordinate(&mut self, coordinate: ChangeCoordinate) {
		self.change_coordinate = Some(coordinate);
	}

	fn flow_watermark(&self) -> Option<DateTime> {
		self.flow_watermark
	}

	fn set_flow_watermark(&mut self, watermark: DateTime) {
		self.flow_watermark = Some(watermark);
	}

	fn source_watermark_cache(&mut self) -> &mut HashMap<OperatorId, i64> {
		&mut self.source_watermark_cache
	}

	fn row_shape_cache(&mut self, operator: OperatorId) -> &mut HashMap<EncodedKey, RowShape> {
		self.row_shape_cache.entry(operator).or_default()
	}

	fn run_durable_sink(&mut self, sink: &mut dyn DurableSink, change: Change) -> Result<Change> {
		sink.apply(self, change)
	}

	fn run_durable_sink_timer(&mut self, sink: &mut dyn DurableSink, timer: Timer) -> Result<Option<Change>> {
		sink.on_timer(self, timer)
	}

	fn storage_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		#[cfg(test)]
		{
			self.storage_counts.gets += 1;
		}
		deferred_storage_get(
			self.substrate.operators.as_ref(),
			self.query.as_ref(),
			self.state_query.as_ref(),
			key,
		)
	}

	fn storage_get_many(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>> {
		#[cfg(test)]
		{
			self.storage_counts.get_many_calls += 1;
		}
		deferred_storage_get_many(
			self.substrate.operators.as_ref(),
			self.query.as_ref(),
			self.state_query.as_ref(),
			keys,
		)
	}

	fn storage_contains(&mut self, key: &EncodedKey) -> Result<bool> {
		deferred_storage_contains(
			self.substrate.operators.as_ref(),
			self.query.as_ref(),
			self.state_query.as_ref(),
			key,
		)
	}

	fn storage_range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		#[cfg(test)]
		{
			self.storage_counts.ranges += 1;
		}
		deferred_storage_range(
			self.substrate.operators.as_ref(),
			self.query.as_ref(),
			self.state_query.as_ref(),
			self.version,
			range,
			scope,
			batch_size,
		)
	}

	fn fetch_state(&mut self, id: OperatorId, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedBytes>>> {
		#[cfg(test)]
		if !keys.is_empty() {
			self.storage_counts.state_fetch_calls += 1;
		}
		deferred_fetch_state(self.substrate.operators.as_ref(), id, keys)
	}
}

impl Get for DeferredTransaction {
	fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		pending_get(self, key)
	}

	fn get_many(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>> {
		pending_get_many(self, keys)
	}
}

impl Batch for DeferredTransaction {
	fn set(&mut self, keys: Vec<EncodedKey>, rows: Vec<EncodedBytes>) -> Result<()> {
		self.pending_mut().insert_batch(keys, rows);
		Ok(())
	}

	fn remove(&mut self, keys: Vec<EncodedKey>) -> Result<()> {
		self.pending_mut().remove_batch(keys);
		Ok(())
	}
}

impl Emit for DeferredTransaction {
	fn emit(&mut self, _view: ViewId, diff: Diff) -> Result<()> {
		self.emitted |= diff.row_count() > 0;
		Ok(())
	}
}

impl Resolve for DeferredTransaction {
	fn dictionary(&mut self, id: DictionaryId) -> Result<Option<Dictionary>> {
		Ok(self.find_dictionary(id))
	}

	fn resolve(&mut self, dictionary: &Dictionary, ids: &[DictionaryEntryId]) -> Result<Vec<Option<Value>>> {
		self.get_many_from_dictionary(dictionary, ids)
	}
}

impl Intern for DeferredTransaction {
	fn find(&mut self, dictionary: &Dictionary, values: &[Value]) -> Result<Vec<Option<DictionaryEntryId>>> {
		self.find_many_in_dictionary(dictionary, values)
	}

	fn intern(&mut self, dictionary: &Dictionary, values: &[Value]) -> Result<Vec<DictionaryEntryId>> {
		if values.is_empty() {
			return Ok(Vec::new());
		}
		let mut prepared = values.to_vec();
		DictionaryRowInterceptor::pre_insert(self, dictionary, &mut prepared)?;

		let outcomes = self.dictionary_allocators().intern_batch(dictionary, &prepared)?;

		let mut seen = HashSet::new();
		let mut created_ids: Vec<DictionaryEntryId> = Vec::new();
		let mut created_values: Vec<Value> = Vec::new();
		for (outcome, value) in outcomes.iter().zip(&prepared) {
			if outcome.created && seen.insert(outcome.id) {
				created_ids.push(outcome.id);
				created_values.push(value.clone());
			}
		}
		if !created_ids.is_empty() {
			DictionaryRowInterceptor::post_insert(self, dictionary, &created_ids, &created_values)?;
		}

		Ok(outcomes.into_iter().map(|outcome| outcome.id).collect())
	}
}

macro_rules! interceptor_method {
	($method:ident, $field:ident, $trait_name:ident) => {
		fn $method(&mut self) -> &mut Chain<dyn $trait_name + Send + Sync> {
			&mut self.interceptors.$field
		}
	};
}

impl WithInterceptors for DeferredTransaction {
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

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::{ArrayRef, Int32Array, RecordBatch};
	use reifydb_core::value::batch::empty_batch;
	use reifydb_test_harness::engine::TestEngine;

	use super::*;
	use crate::transaction::mock::FlowTxn;

	#[test]
	fn emit_marks_the_transaction_only_for_a_diff_with_rows() {
		// The flag decides whether a step commits; emit owns that rule because an emitter may send an empty
		// diff, and an empty diff that set the flag would commit a step that changed nothing.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().clock_millis(0).deferred();

		txn.emit(ViewId(1), Diff::insert(empty_batch())).unwrap();
		assert!(!txn.emitted, "an empty diff must not mark the transaction emitted");

		let column: ArrayRef = Arc::new(Int32Array::from(vec![1]));
		txn.emit(ViewId(1), Diff::insert(RecordBatch::try_from_iter([("c", column)]).unwrap())).unwrap();
		assert!(txn.emitted, "a diff with a row must mark the transaction emitted");
	}
}
