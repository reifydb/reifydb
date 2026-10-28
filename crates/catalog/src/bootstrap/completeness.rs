// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::TimeSource,
	durability::Durability,
	event::EventBus,
	interface::catalog::id::{ColumnId, NamespaceId, TableId},
};
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::{
	fragment::Fragment,
	value::{identity::IdentityId, value_type::ValueType},
};
use tracing::info;

use super::{ensure_namespace, table_col};
use crate::{
	Result,
	cache::CatalogCache,
	catalog::{Catalog, table::TableToCreate},
};

pub fn bootstrap_completeness(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &CatalogCache,
	eventbus: &EventBus,
) -> Result<()> {
	let catalog_api = Catalog::new(catalog.clone());
	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
		Durability::Async,
	)?;

	let ns = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_SOURCE,
		"system::source",
		"source",
		NamespaceId::SYSTEM,
	)?;

	if let Some(table) = catalog_api.find_table_by_name(&mut Transaction::Admin(&mut admin), ns, "completeness")? {
		if catalog_api.find_primary_key(&mut Transaction::Admin(&mut admin), table.id)?.is_none() {
			panic!(
				"system::source::completeness has no primary key on object_id; this store predates it, wipe the data dir"
			);
		}
	} else {
		catalog_api.create_table_with_id(
			&mut admin,
			TableId::SOURCE_COMPLETENESS,
			TableToCreate {
				name: Fragment::internal("completeness"),
				namespace: ns,
				columns: vec![
					table_col("object_id", ValueType::Uint8),
					table_col("complete_through", ValueType::DateTime),
				],
				primary_key_columns: Some(vec!["object_id".to_string()]),
				partition_by: vec![],
				time: TimeSource::None,
			},
			&ColumnId::SOURCE_COMPLETENESS_COLUMNS,
		)?;
		info!("Created system::source::completeness table");
	}

	admin.commit()?;
	Ok(())
}
