// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::row_number::RowNumber;

use crate::encoded::{row::EncodedRow, schema::RowSchema};

#[derive(Debug, Clone)]
pub struct Row {
	pub number: RowNumber,
	pub encoded: EncodedRow,
	pub schema: RowSchema,
}
