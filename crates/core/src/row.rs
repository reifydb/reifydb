// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::row_number::RowNumber;

use crate::encoded::{encoded::EncodedValues, schema::Schema};

#[derive(Debug, Clone)]
pub struct Row {
	pub number: RowNumber,
	pub encoded: EncodedValues,
	pub schema: Schema,
}
