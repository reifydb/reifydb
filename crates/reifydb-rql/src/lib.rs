// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_core::{
	Result, Type, result::error::diagnostic::ast::unrecognized_type,
	return_error,
};

use crate::ast::AstIdentifier;

pub mod ast;
pub mod explain;
pub mod expression;
#[allow(dead_code, unused_variables)]
pub mod flow;
pub mod plan;

pub(crate) fn convert_data_type(ast: &AstIdentifier) -> Result<Type> {
	Ok(match ast.value().to_ascii_lowercase().as_str() {
		"bool" => Type::Bool,
		"float4" => Type::Float4,
		"float8" => Type::Float8,
		"int1" => Type::Int1,
		"int2" => Type::Int2,
		"int4" => Type::Int4,
		"int8" => Type::Int8,
		"int16" => Type::Int16,
		"uint1" => Type::Uint1,
		"uint2" => Type::Uint2,
		"uint4" => Type::Uint4,
		"uint8" => Type::Uint8,
		"uint16" => Type::Uint16,
		"utf8" => Type::Utf8,
		"text" => Type::Utf8,
		"date" => Type::Date,
		"datetime" => Type::DateTime,
		"time" => Type::Time,
		"interval" => Type::Interval,
		"uuid4" => Type::Uuid4,
		"uuid7" => Type::Uuid7,
		"blob" => Type::Blob,
		_ => return_error!(unrecognized_type(ast.clone().fragment())),
	})
}
