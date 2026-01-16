// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::row_number::RowNumber;

use crate::encoded::{encoded::EncodedValues, named::EncodedValuesNamedLayout};

#[derive(Debug, Clone)]
pub struct Row {
	pub number: RowNumber,
	pub encoded: EncodedValues,
	pub layout: EncodedValuesNamedLayout,
}
