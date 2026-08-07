// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::TimeSource,
	event::EventBus,
	interface::catalog::id::{ColumnId, NamespaceId, TableId},
};
use reifydb_runtime::context::clock::Clock;
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
		Clock::Real,
	)?;

	let ns = ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_SOURCE,
		"system::source",
		"source",
		NamespaceId::SYSTEM,
	)?;

	if catalog_api.find_table_by_name(&mut Transaction::Admin(&mut admin), ns, "completeness")?.is_none() {
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
				primary_key_columns: None,
				partition_by: vec![],
				underlying: false,
				time: TimeSource::None,
			},
			&ColumnId::SOURCE_COMPLETENESS_COLUMNS,
		)?;
		info!("Created system::source::completeness table");
	}

	admin.commit()?;
	Ok(())
}
