// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::config::SystemConfig;

pub(crate) fn register_defaults(config: &SystemConfig) {
	reifydb_transaction::register_oracle_defaults(config);
}
