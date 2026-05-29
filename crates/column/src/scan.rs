// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_value::Result;

pub trait ScanSource {
	fn next_batch(&mut self) -> Result<Option<Columns>>;
}
