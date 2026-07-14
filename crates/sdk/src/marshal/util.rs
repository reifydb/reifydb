// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_codec::column_type::type_code_of;
use reifydb_core::value::column::buffer::ColumnBuffer;

pub(crate) fn column_data_to_type_code(data: &ColumnBuffer) -> ColumnTypeCode {
	type_code_of(&data.get_type())
}
