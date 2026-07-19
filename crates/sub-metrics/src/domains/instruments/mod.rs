// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::vtable::user::UserVTableColumn;
use reifydb_core::{
	interface::catalog::id::NamespaceId, metrics::registry::MetricsRegistry, value::column::columns::Columns,
};
use reifydb_value::value::datetime::DateTime;

use crate::{
	domains::runtime::{runtime_columns, samples_to_columns},
	framework::source::MetricsSource,
};

pub struct InstrumentsSource {
	registry: MetricsRegistry,
}

impl InstrumentsSource {
	pub fn new(registry: MetricsRegistry) -> Self {
		Self {
			registry,
		}
	}
}

impl MetricsSource for InstrumentsSource {
	fn namespace(&self) -> NamespaceId {
		NamespaceId::SYSTEM_METRICS_INSTRUMENTS
	}

	fn columns(&self) -> Vec<UserVTableColumn> {
		runtime_columns()
	}

	fn collect(&self, now: DateTime) -> Columns {
		samples_to_columns(&self.registry.read_reporters(), now)
	}
}
