// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::data::column::ColumnTypeCode;
use reifydb_value::value::value_type::ValueType;

use crate::tag::{TypeTag, ValueKind};

pub fn type_code_of(ty: &ValueType) -> ColumnTypeCode {
	code_of_kind(ValueKind::of_type(ty))
}

pub fn value_type_of(code: ColumnTypeCode) -> Option<ValueType> {
	let kind = ValueKind::from_byte(code.byte())?;
	TypeTag::new(kind, 0).ok()?.to_type().ok()
}

fn code_of_kind(kind: ValueKind) -> ColumnTypeCode {
	ColumnTypeCode::from_u8(kind.byte()).unwrap_or(ColumnTypeCode::Any)
}
