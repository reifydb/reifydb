// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

use reifydb_type::{
	Result,
	error::diagnostic::ast::unrecognized_type,
	fragment::Fragment,
	return_error,
	value::{
		constraint::{Constraint, TypeConstraint},
		r#type::Type,
	},
};

use crate::ast::ast::{AstDataType, AstLiteral};

pub mod ast;
pub mod error;
pub mod explain;
pub mod expression;
#[allow(dead_code, unused_variables)]
pub mod flow;
pub mod plan;

pub(crate) fn convert_data_type(ast: &Fragment) -> Result<Type> {
	Ok(match ast.text().to_ascii_lowercase().as_str() {
		"bool" => Type::Boolean,
		"boolean" => Type::Boolean,
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
		"duration" => Type::Duration,
		"uuid4" => Type::Uuid4,
		"uuid7" => Type::Uuid7,
		"blob" => Type::Blob,
		"int" => Type::Int,
		"uint" => Type::Uint,
		"decimal" => Type::Decimal,
		_ => return_error!(unrecognized_type(ast.clone())),
	})
}

pub(crate) fn convert_data_type_with_constraints(ast: &AstDataType) -> Result<TypeConstraint> {
	match ast {
		AstDataType::Unconstrained(name) => {
			let base_type = convert_data_type(name)?;
			Ok(TypeConstraint::unconstrained(base_type))
		}
		AstDataType::Constrained {
			name,
			params,
		} => {
			let base_type = convert_data_type(name)?;

			// Parse constraint based on type and parameters
			let constraint = match (base_type, params.as_slice()) {
				(Type::Utf8, [AstLiteral::Number(n)]) => {
					let max_bytes = parse_number_literal(n.value())? as u32;
					Some(Constraint::MaxBytes(max_bytes.into()))
				}
				(Type::Blob, [AstLiteral::Number(n)]) => {
					let max_bytes = parse_number_literal(n.value())? as u32;
					Some(Constraint::MaxBytes(max_bytes.into()))
				}
				(Type::Int, [AstLiteral::Number(n)]) => {
					let max_bytes = parse_number_literal(n.value())? as u32;
					Some(Constraint::MaxBytes(max_bytes.into()))
				}
				(Type::Uint, [AstLiteral::Number(n)]) => {
					let max_bytes = parse_number_literal(n.value())? as u32;
					Some(Constraint::MaxBytes(max_bytes.into()))
				}
				(Type::Decimal, [AstLiteral::Number(p), AstLiteral::Number(s)]) => {
					let precision = parse_number_literal(p.value())? as u8;
					let scale = parse_number_literal(s.value())? as u8;
					Some(Constraint::PrecisionScale(precision.into(), scale.into()))
				}
				// Type doesn't support constraints or invalid
				// parameter count
				_ => None,
			};

			match constraint {
				Some(c) => Ok(TypeConstraint::with_constraint(base_type, c)),
				None => Ok(TypeConstraint::unconstrained(base_type)),
			}
		}
	}
}

fn parse_number_literal(s: &str) -> Result<usize> {
	s.parse::<usize>().map_err(|_| {
		reifydb_type::error!(reifydb_core::error::diagnostic::internal::internal(format!(
			"Invalid number literal: {}",
			s
		)))
	})
}

use reifydb_core::interface::version::{ComponentType, HasVersion, SystemVersion};

pub struct RqlVersion;

impl HasVersion for RqlVersion {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: env!("CARGO_PKG_NAME")
				.strip_prefix("reifydb-")
				.unwrap_or(env!("CARGO_PKG_NAME"))
				.to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "ReifyDB Query Language parser and planner module".to_string(),
			r#type: ComponentType::Module,
		}
	}
}
