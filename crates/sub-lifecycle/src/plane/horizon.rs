// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::catalog::config::{ConfigKey, GetConfig},
	lifecycle::operator::ListOperatorSettings,
	row::Ttl,
};
use reifydb_value::value::duration::Duration;

pub fn max_retention_horizon(catalog: &Catalog) -> Duration {
	let floor = catalog.get_config_duration(ConfigKey::MaxRetentionHorizonFloor);

	let rows = catalog.list_row_settings().into_iter().filter_map(|(_, settings)| settings.ttl);

	let operators = catalog.list_operator_settings().into_iter().flat_map(|(_, settings)| {
		let join = settings.join.into_iter().flat_map(|join| [join.left, join.right]);
		settings.ttl.into_iter().chain(join.flatten())
	});

	rows.chain(operators).map(|ttl: Ttl| ttl.duration).fold(floor, |longest, declared| longest.max(declared))
}
