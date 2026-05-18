// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::datetime::DateTime;

#[derive(Debug, Clone)]
pub enum OperatorTtlMessage {
	Tick(DateTime),

	Shutdown,
}
