// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::catalog::{
	config::{ConfigKey, GetConfig},
	storage::StorageId,
};
use reifydb_value::value::duration::Duration;

pub fn history_horizon(catalog: &Catalog) -> Duration {
	let minimum = catalog.get_config_duration(ConfigKey::MinHistoryHorizon);

	let rows = catalog
		.list_row_settings()
		.into_iter()
		.filter(|(storage, _)| !matches!(storage, StorageId::View(_)))
		.filter_map(|(_, settings)| settings.ttl)
		.map(|ttl| ttl.duration);

	rows.fold(minimum, |longest, declared| longest.max(declared))
}
