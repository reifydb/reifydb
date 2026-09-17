// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::operator::ListOperatorRetention,
};
use reifydb_value::value::duration::Duration;

pub fn max_retention_horizon(catalog: &Catalog) -> Duration {
	let floor = catalog.get_config_duration(ConfigKey::MaxRetentionHorizonFloor);

	let rows = catalog
		.list_row_settings()
		.into_iter()
		.filter_map(|(_, settings)| settings.ttl)
		.map(|ttl| ttl.duration);

	let operators = catalog.list_operator_retention().into_iter().map(|(_, retention)| retention.duration);

	rows.chain(operators).fold(floor, |longest, declared| longest.max(declared))
}
