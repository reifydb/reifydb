// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Literal value compilation.

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::fragment::Fragment;

use crate::expression::types::CompiledExpr;

pub(super) fn compile_literal_undefined() -> CompiledExpr {
	CompiledExpr::new(|columns, _ctx| {
		let row_count = columns.row_count();
		Ok(Column::new(Fragment::internal("_undefined"), ColumnData::undefined(row_count)))
	})
}

pub(super) fn compile_literal_bool(value: bool) -> CompiledExpr {
	CompiledExpr::new(move |columns, _ctx| {
		let row_count = columns.row_count();
		Ok(Column::new(Fragment::internal("_bool"), ColumnData::bool(vec![value; row_count])))
	})
}

pub(super) fn compile_literal_int(value: i64) -> CompiledExpr {
	CompiledExpr::new(move |columns, _ctx| {
		let row_count = columns.row_count();
		Ok(Column::new(Fragment::internal("_int"), ColumnData::int8(vec![value; row_count])))
	})
}

pub(super) fn compile_literal_float(value: f64) -> CompiledExpr {
	CompiledExpr::new(move |columns, _ctx| {
		let row_count = columns.row_count();
		Ok(Column::new(
			Fragment::internal("_float"),
			ColumnData::float8(std::iter::repeat(value).take(row_count)),
		))
	})
}

pub(super) fn compile_literal_string(value: String) -> CompiledExpr {
	CompiledExpr::new(move |columns, _ctx| {
		let value = value.clone();
		let row_count = columns.row_count();
		Ok(Column::new(
			Fragment::internal("_string"),
			ColumnData::utf8(std::iter::repeat(value).take(row_count).collect::<Vec<_>>()),
		))
	})
}

pub(super) fn compile_literal_bytes(_value: Vec<u8>) -> CompiledExpr {
	// TODO: Implement proper bytes column support
	CompiledExpr::new(|columns, _ctx| {
		let row_count = columns.row_count();
		Ok(Column::new(Fragment::internal("_bytes"), ColumnData::undefined(row_count)))
	})
}
