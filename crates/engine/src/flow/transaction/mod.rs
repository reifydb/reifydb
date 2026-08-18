// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_assertions)]
use std::{any::type_name, collections::HashSet};
use std::{
	any::{Any, TypeId},
	collections::HashMap,
	mem,
	sync::Arc,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{flow::OperatorId, object::ObjectId},
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
			IdentityPostCreateInterceptor, IdentityPreDeleteInterceptor,
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
			RolePostCreateInterceptor, RolePreDeleteInterceptor,
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
	transaction::Transaction,
};
use reifydb_value::{Result, reifydb_assertions};
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

use crate::flow::host::{HostCatalog, StandardHostCatalog};

pub struct FlowTransactionInner {
	pub catalog: Catalog,
	pub host_catalog: Arc<dyn HostCatalog>,
	pub interceptors: Interceptors,
	pub accumulator: ChangeAccumulator,
	pub clock: Clock,

	pub operator_states: HashMap<OperatorId, OperatorStateSlot>,

	pub operator_caches: HashMap<(OperatorId, TypeId), Box<dyn Any + Send>>,

	#[cfg(reifydb_assertions)]
	pub checked_out_caches: HashSet<(OperatorId, TypeId)>,

	pub store_reads: u64,

	pub allocators: FlowAllocators,
}

pub struct FlowTransaction<'a, 'b> {
	pub(crate) txn: &'a mut Transaction<'b>,
	inner: FlowTransactionInner,
}

impl<'a, 'b> FlowTransaction<'a, 'b> {
	pub fn new(
		txn: &'a mut Transaction<'b>,
		catalog: Catalog,
		interceptors: Interceptors,
		clock: Clock,
		allocators: FlowAllocators,
	) -> Self {
		Self {
			txn,
			inner: FlowTransactionInner {
				catalog: catalog.clone(),
				host_catalog: Arc::new(StandardHostCatalog::new(catalog)),
				interceptors,
				accumulator: ChangeAccumulator::new(),
				clock,
				operator_states: HashMap::new(),
				operator_caches: HashMap::new(),
				#[cfg(reifydb_assertions)]
				checked_out_caches: HashSet::new(),
				store_reads: 0,
				allocators,
			},
		}
	}

	pub(crate) fn inner_mut(&mut self) -> &mut FlowTransactionInner {
		&mut self.inner
	}

	pub fn row_allocators(&self) -> RowAllocatorRegistry {
		self.inner.allocators.row.clone()
	}

	pub fn dictionary_allocators(&self) -> DictionaryAllocatorRegistry {
		self.inner.allocators.dictionary.clone()
	}

	pub fn version(&self) -> CommitVersion {
		self.txn.version()
	}

	pub fn store_reads(&self) -> u64 {
		self.inner.store_reads
	}

	pub fn track_flow_change(&mut self, change: Change) {
		if let ChangeOrigin::Object(id) = change.origin {
			for diff in change.diffs {
				self.inner.accumulator.track(id, diff);
			}
		}
	}

	pub fn take_accumulator_entries(&mut self) -> Vec<(ObjectId, Diff)> {
		let acc = &mut self.inner.accumulator;
		let entries: Vec<_> = acc.entries_from(0).to_vec();
		acc.clear();
		entries
	}

	pub fn catalog(&self) -> &Catalog {
		&self.inner.catalog
	}

	pub fn host_catalog(&self) -> &dyn HostCatalog {
		&*self.inner.host_catalog
	}

	pub fn clock(&self) -> &Clock {
		&self.inner.clock
	}

	pub fn operator_state<S, F>(&mut self, node: OperatorId, load: F) -> Result<&mut S>
	where
		S: 'static + Send,
		F: FnOnce(&mut Self) -> Result<(S, PersistFn)>,
	{
		if !self.inner.operator_states.contains_key(&node) {
			let (state, persist) = load(self)?;
			let slot = OperatorStateSlot {
				value: Box::new(state),
				dirty: false,
				persist,
			};
			self.inner.operator_states.insert(node, slot);
		}
		let slot = self.inner.operator_states.get_mut(&node).expect("just inserted");
		Ok(slot.value.downcast_mut::<S>().expect("operator state type mismatch"))
	}

	pub fn mark_state_dirty(&mut self, node: OperatorId) {
		if let Some(slot) = self.inner.operator_states.get_mut(&node) {
			slot.dirty = true;
		}
	}

	pub fn take_operator_state<S, F>(&mut self, node: OperatorId, load: F) -> Result<(S, PersistFn)>
	where
		S: 'static + Send,
		F: FnOnce(&mut Self) -> Result<(S, PersistFn)>,
	{
		if let Some(slot) = self.inner.operator_states.remove(&node) {
			let value = slot.value.downcast::<S>().map_err(|_| ()).expect("operator state type mismatch");
			Ok((*value, slot.persist))
		} else {
			load(self)
		}
	}

	pub fn put_operator_state<S>(&mut self, node: OperatorId, state: S, persist: PersistFn)
	where
		S: 'static + Send,
	{
		self.inner.operator_states.insert(
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
		let states = mem::take(&mut self.inner.operator_states);
		for (_, slot) in states {
			if slot.dirty {
				(slot.persist)(self, slot.value)?;
			}
		}
		Ok(())
	}

	pub fn take_cache<C>(&mut self, node: OperatorId) -> C
	where
		C: 'static + Send + Default,
	{
		let key = (node, TypeId::of::<C>());
		reifydb_assertions! {
			assert!(
				self.inner.checked_out_caches.insert(key),
				"operator {} took its {} cache twice without putting it back, so the inner copy would \
				 be discarded by the outer put and its memo entries lost",
				node.0,
				type_name::<C>()
			);
		}
		match self.inner.operator_caches.remove(&key) {
			Some(cache) => *cache.downcast::<C>().expect("operator cache keyed by its type id"),
			None => C::default(),
		}
	}

	pub fn put_cache<C>(&mut self, node: OperatorId, cache: C)
	where
		C: 'static + Send,
	{
		let key = (node, TypeId::of::<C>());
		reifydb_assertions! {
			assert!(
				self.inner.checked_out_caches.remove(&key),
				"operator {} put back a {} cache it never took, so it would silently overwrite the copy \
				 another caller still holds",
				node.0,
				type_name::<C>()
			);
		}
		self.inner.operator_caches.insert(key, Box::new(cache));
	}

	pub fn install_operator_states(&mut self, states: HashMap<OperatorId, Box<dyn Any + Send>>) {
		for (node, value) in states {
			self.inner.operator_states.entry(node).or_insert_with(|| OperatorStateSlot {
				value,
				dirty: false,
				persist: Box::new(|_, _| Ok(())),
			});
		}
	}

	pub fn drain_operator_states(&mut self) -> HashMap<OperatorId, Box<dyn Any + Send>> {
		mem::take(&mut self.inner.operator_states)
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

impl WithInterceptors for FlowTransaction<'_, '_> {
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
