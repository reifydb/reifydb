// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound::{Excluded, Included, Unbounded};

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
};
use reifydb_core::{
	actors::pending::PendingLayers,
	common::CommitVersion,
	interface::{change::Change, store::MultiVersionRow},
};
use reifydb_runtime::context::clock::Clock;
use reifydb_store_operator::store::OperatorStore;
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

use crate::{
	operator::sink::DurableSink,
	timer::Timer,
	transaction::{
		ChangeCoordinate, DeferredParams, FlowTransaction,
		read::{OperatorStateRangeIter, ReadFrom, read_from},
		scope::{OperatorRangeScope, OperatorScope, operator_state_coordinates, operator_state_scope},
		substrate::FlowSubstrate,
	},
};

pub struct DeferredTransaction {
	pub version: CommitVersion,
	pub pending: PendingLayers,
	pub query: MultiReadTransaction,
	pub state_query: MultiReadTransaction,
	pub catalog: Catalog,
	pub interceptors: Interceptors,
	pub accumulator: ChangeAccumulator,
	pub clock: Clock,

	pub change_coordinate: Option<ChangeCoordinate>,

	pub flow_watermark: Option<DateTime>,

	pub substrate: FlowSubstrate,
}

impl DeferredTransaction {
	#[instrument(name = "flow::transaction::deferred", level = "debug", skip(parent, catalog, interceptors, clock), fields(version = version.0))]
	pub fn new(
		parent: &AdminTransaction,
		version: CommitVersion,
		catalog: Catalog,
		interceptors: Interceptors,
		clock: Clock,
	) -> Self {
		let mut query = parent.multi.begin_query().unwrap();
		query.read_as_of_version_inclusive(version);

		let state_query = parent.multi.begin_query().unwrap();

		Self {
			version,
			pending: PendingLayers::empty(),
			query,
			state_query,
			catalog,
			interceptors,
			accumulator: ChangeAccumulator::new(),
			clock,
			change_coordinate: None,
			flow_watermark: None,
			substrate: FlowSubstrate::new(),
		}
	}

	pub fn from_parts(params: DeferredParams) -> Self {
		let mut query = params.query;
		query.read_as_of_version_inclusive(params.version);

		Self {
			version: params.version,
			pending: params.pending,
			query,
			state_query: params.state_query,
			catalog: params.catalog,
			interceptors: params.interceptors,
			accumulator: ChangeAccumulator::new(),
			clock: params.clock,
			change_coordinate: None,
			flow_watermark: None,
			substrate: params.substrate,
		}
	}
}

pub(crate) fn deferred_storage_get(
	operators: &OperatorStore,
	query: &MultiReadTransaction,
	state_query: &MultiReadTransaction,
	key: &EncodedKey,
) -> Result<Option<EncodedBytes>> {
	let route = read_from(key);
	if matches!(route, ReadFrom::OperatorState) {
		let OperatorScope {
			operator,
			inner,
		} = operator_state_coordinates(key).expect("an OperatorState-routed key must carry an operator id");
		return Ok(operators.get(operator, &inner).map(EncodedOperatorRow::into_bytes));
	}
	let query = match route {
		ReadFrom::StateQuery | ReadFrom::OwnedRow => state_query,
		ReadFrom::Query => query,
		ReadFrom::OperatorState => unreachable!(),
	};
	Ok(query.get(key)?.map(|multi| multi.bytes().clone()))
}

pub(crate) fn deferred_storage_contains(
	operators: &OperatorStore,
	query: &MultiReadTransaction,
	state_query: &MultiReadTransaction,
	key: &EncodedKey,
) -> Result<bool> {
	let query = match read_from(key) {
		ReadFrom::OperatorState => {
			let OperatorScope {
				operator,
				inner,
			} = operator_state_coordinates(key)
				.expect("an OperatorState-routed key must carry an operator id");
			return Ok(operators.contains(operator, &inner));
		}
		ReadFrom::StateQuery | ReadFrom::OwnedRow => state_query,
		ReadFrom::Query => query,
	};
	query.contains_key(key)
}

pub(crate) fn deferred_storage_range<'a>(
	operators: &OperatorStore,
	query: &'a MultiReadTransaction,
	state_query: &'a MultiReadTransaction,
	version: CommitVersion,
	range: EncodedKeyRange,
	scope: RangeScope,
	batch_size: usize,
) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a> {
	if let Some(OperatorRangeScope {
		operator,
		inner,
	}) = operator_state_scope(&range)
	{
		return Box::new(OperatorStateRangeIter::new(operators.clone(), operator, inner, batch_size, version));
	}
	let query = deferred_range_target(query, state_query, &range);
	Box::new(query.range(range, scope, batch_size))
}

pub(crate) fn deferred_storage_range_rev<'a>(
	operators: &OperatorStore,
	query: &'a MultiReadTransaction,
	state_query: &'a MultiReadTransaction,
	version: CommitVersion,
	range: EncodedKeyRange,
	scope: RangeScope,
	batch_size: usize,
) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + 'a> {
	if let Some(OperatorRangeScope {
		operator,
		inner,
	}) = operator_state_scope(&range)
	{
		let mut items = OperatorStateRangeIter::new(operators.clone(), operator, inner, batch_size, version)
			.collect::<Vec<_>>();
		items.reverse();
		return Box::new(items.into_iter());
	}
	let query = deferred_range_target(query, state_query, &range);
	Box::new(query.range_rev(range, scope, batch_size))
}

fn deferred_range_target<'a>(
	query: &'a MultiReadTransaction,
	state_query: &'a MultiReadTransaction,
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
}

pub(crate) fn deferred_fetch_state_external(
	operators: &OperatorStore,
	version: CommitVersion,
	keys: &[EncodedKey],
	items: &mut Vec<MultiVersionRow>,
) {
	for encoded_key in keys {
		let OperatorScope {
			operator,
			inner,
		} = operator_state_coordinates(encoded_key).expect("state_get_many keys must carry an operator id");
		if let Some(row) = operators.get(operator, &inner) {
			items.push(MultiVersionRow {
				key: encoded_key.clone(),
				bytes: row.into_bytes(),
				version,
			});
		}
	}
}

impl FlowTransaction for DeferredTransaction {
	fn version(&self) -> CommitVersion {
		self.version
	}

	fn clock(&self) -> &Clock {
		&self.clock
	}

	fn catalog(&self) -> &Catalog {
		&self.catalog
	}

	fn query(&self) -> MultiReadTransaction {
		self.query.clone()
	}

	fn substrate(&self) -> &FlowSubstrate {
		&self.substrate
	}

	fn pending_layers(&self) -> &PendingLayers {
		&self.pending
	}

	fn pending_layers_mut(&mut self) -> &mut PendingLayers {
		&mut self.pending
	}

	fn accumulator_mut(&mut self) -> &mut ChangeAccumulator {
		&mut self.accumulator
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

	fn run_durable_sink(&mut self, sink: &mut dyn DurableSink, change: Change) -> Result<Change> {
		sink.apply(self, change)
	}

	fn run_durable_sink_timer(&mut self, sink: &mut dyn DurableSink, timer: Timer) -> Result<Option<Change>> {
		sink.on_timer(self, timer)
	}

	fn storage_get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		deferred_storage_get(&self.substrate.operators, &self.query, &self.state_query, key)
	}

	fn storage_contains(&mut self, key: &EncodedKey) -> Result<bool> {
		deferred_storage_contains(&self.substrate.operators, &self.query, &self.state_query, key)
	}

	fn storage_range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		deferred_storage_range(
			&self.substrate.operators,
			&self.query,
			&self.state_query,
			self.version,
			range,
			scope,
			batch_size,
		)
	}

	fn storage_range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		deferred_storage_range_rev(
			&self.substrate.operators,
			&self.query,
			&self.state_query,
			self.version,
			range,
			scope,
			batch_size,
		)
	}

	fn fetch_state_external(&mut self, keys: &[EncodedKey], items: &mut Vec<MultiVersionRow>) -> Result<()> {
		deferred_fetch_state_external(&self.substrate.operators, self.version, keys, items);
		Ok(())
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
