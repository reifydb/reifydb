// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	id::NamespaceId,
	reducer::{ReducerActionDef, ReducerActionId, ReducerDef, ReducerId},
};
use reifydb_transaction::transaction::{AsTransaction, admin::AdminTransaction};
use reifydb_type::{fragment::Fragment, value::constraint::TypeConstraint};
use tracing::instrument;

use crate::{
	CatalogStore,
	catalog::Catalog,
	store::{reducer::create::ReducerToCreate as StoreReducerToCreate, sequence::reducer as reducer_sequence},
};

#[derive(Debug, Clone)]
pub struct ReducerColumnToCreate {
	pub name: String,
	pub constraint: TypeConstraint,
	pub fragment: Option<Fragment>,
}

#[derive(Debug, Clone)]
pub struct ReducerToCreate {
	pub fragment: Option<Fragment>,
	pub name: String,
	pub namespace: NamespaceId,
	pub key_columns: Vec<String>,
}

impl From<ReducerToCreate> for StoreReducerToCreate {
	fn from(to_create: ReducerToCreate) -> Self {
		StoreReducerToCreate {
			fragment: to_create.fragment,
			name: to_create.name,
			namespace: to_create.namespace,
			key_columns: to_create.key_columns,
		}
	}
}

impl Catalog {
	#[instrument(name = "catalog::reducer::find_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_reducer_by_name<T: AsTransaction>(
		&self,
		txn: &mut T,
		namespace: NamespaceId,
		name: &str,
	) -> crate::Result<Option<ReducerDef>> {
		CatalogStore::find_reducer_by_name(txn, namespace, name)
	}

	#[instrument(name = "catalog::reducer::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_reducer(
		&self,
		txn: &mut AdminTransaction,
		to_create: ReducerToCreate,
	) -> crate::Result<ReducerDef> {
		CatalogStore::create_reducer(txn, to_create.into())
	}

	#[instrument(name = "catalog::reducer::next_id", level = "trace", skip(self, txn))]
	pub fn next_reducer_id(&self, txn: &mut AdminTransaction) -> crate::Result<ReducerId> {
		reducer_sequence::next_reducer_id(txn)
	}

	#[instrument(name = "catalog::reducer::next_action_id", level = "trace", skip(self, txn))]
	pub fn next_reducer_action_id(&self, txn: &mut AdminTransaction) -> crate::Result<ReducerActionId> {
		reducer_sequence::next_reducer_action_id(txn)
	}

	#[instrument(name = "catalog::reducer::create_action", level = "debug", skip(self, txn, action_def))]
	pub fn create_reducer_action(
		&self,
		txn: &mut AdminTransaction,
		action_def: &ReducerActionDef,
	) -> crate::Result<()> {
		CatalogStore::create_reducer_action(txn, action_def)
	}

	#[instrument(name = "catalog::reducer::find_action_by_name", level = "trace", skip(self, txn, name))]
	pub fn find_reducer_action_by_name<T: AsTransaction>(
		&self,
		txn: &mut T,
		reducer_id: ReducerId,
		name: &str,
	) -> crate::Result<Option<ReducerActionDef>> {
		CatalogStore::find_reducer_action_by_name(txn, reducer_id, name)
	}

	#[instrument(name = "catalog::reducer::list_actions", level = "trace", skip(self, txn))]
	pub fn list_reducer_actions<T: AsTransaction>(
		&self,
		txn: &mut T,
		reducer_id: ReducerId,
	) -> crate::Result<Vec<ReducerActionDef>> {
		CatalogStore::list_reducer_actions(txn, reducer_id)
	}

	#[instrument(name = "catalog::reducer::delete_action", level = "debug", skip(self, txn, action_def))]
	pub fn delete_reducer_action(
		&self,
		txn: &mut AdminTransaction,
		action_def: &ReducerActionDef,
	) -> crate::Result<()> {
		CatalogStore::delete_reducer_action(txn, action_def)
	}
}
