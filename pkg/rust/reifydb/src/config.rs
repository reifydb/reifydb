// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::config::SystemConfig;

pub(crate) fn register_defaults(config: &SystemConfig) {
	reifydb_transaction::multi::transaction::register_oracle_defaults(config);
}
