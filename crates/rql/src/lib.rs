// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! ReifyDB Query Language: lexer, parser, AST, logical and physical planning, optimisation, and the instruction stream
//! consumed by the engine VM. This is the full pipeline that turns a source string into something executable.
//!
//! The crate exposes the AST shape so external tooling (formatters, linters, the explain renderer) can inspect queries
//! without re-parsing, and exposes a stable fingerprint over compiled plans that the engine uses for plan caching.
//! Diagnostics produced here carry source-fragment context so user-visible failures can point at the offending span.
//!
//! Invariant: planner output is decoupled from any specific storage backend. The physical plan is expressed in terms
//! of `core::interface/` traits; concrete backend selection happens later, in the engine. Reaching into a specific
//! backend from inside the planner couples this crate to the storage tier and breaks the layering that lets `core`
//! avoid cycles.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::internal_error;
use reifydb_value::{
	Result,
	value::{
		constraint::{Constraint, TypeConstraint, dimension::Dimension},
		value_type::ValueType,
	},
};

use crate::{
	ast::ast::{AstLiteral, AstType},
	bump::BumpFragment,
	diagnostic::AstError,
};

pub mod ast;
pub mod bump;
pub mod compiler;
pub mod diagnostic;
pub mod error;
pub mod expression;
pub mod fingerprint;
#[allow(dead_code, unused_variables)]
pub mod flow;
pub mod instruction;
pub mod nodes;
pub mod optimize;
pub mod plan;
pub mod query;
pub mod token;

pub(crate) fn convert_data_type(ast: &BumpFragment<'_>) -> Result<ValueType> {
	Ok(match ast.text().to_ascii_lowercase().as_str() {
		"bool" => ValueType::Boolean,
		"boolean" => ValueType::Boolean,
		"float4" => ValueType::Float4,
		"float8" => ValueType::Float8,
		"int1" => ValueType::Int1,
		"int2" => ValueType::Int2,
		"int4" => ValueType::Int4,
		"int8" => ValueType::Int8,
		"int16" => ValueType::Int16,
		"uint1" => ValueType::Uint1,
		"uint2" => ValueType::Uint2,
		"uint4" => ValueType::Uint4,
		"uint8" => ValueType::Uint8,
		"uint16" => ValueType::Uint16,
		"utf8" => ValueType::Utf8,
		"text" => ValueType::Utf8,
		"date" => ValueType::Date,
		"datetime" => ValueType::DateTime,
		"time" => ValueType::Time,
		"duration" => ValueType::Duration,
		"uuid4" => ValueType::Uuid4,
		"uuid7" => ValueType::Uuid7,
		"identityid" | "identity_id" => ValueType::IdentityId,
		"blob" => ValueType::Blob,
		"int" => ValueType::Int,
		"uint" => ValueType::Uint,
		"decimal" => ValueType::Decimal,
		// convert_data_type only sees the type name; the real dimension arrives as a constraint
		// param and TypeConstraint::with_constraint rebuilds the parameterized type from it.
		"vector" => ValueType::Vector(0),
		_ => {
			return Err(AstError::UnrecognizedType {
				fragment: ast.to_owned(),
			}
			.into());
		}
	})
}

pub(crate) fn convert_data_type_with_constraints(ast: &AstType) -> Result<TypeConstraint> {
	match ast {
		AstType::Unconstrained(name) => {
			let base_type = convert_data_type(name)?;
			// A vector's dimension is part of its type: without it the column has no stride.
			if matches!(base_type, ValueType::Vector(_)) {
				return Err(AstError::VectorDimensionRequired {
					fragment: name.to_owned(),
				}
				.into());
			}
			Ok(TypeConstraint::unconstrained(base_type))
		}
		AstType::Constrained {
			name,
			params,
		} => {
			let base_type = convert_data_type(name)?;

			let constraint = match (base_type.clone(), params.as_slice()) {
				(ValueType::Utf8, [AstLiteral::Number(n)]) => {
					let max_bytes = parse_number_literal(n.value())? as u32;
					Some(Constraint::MaxBytes(max_bytes.into()))
				}
				(ValueType::Blob, [AstLiteral::Number(n)]) => {
					let max_bytes = parse_number_literal(n.value())? as u32;
					Some(Constraint::MaxBytes(max_bytes.into()))
				}
				(ValueType::Int, [AstLiteral::Number(n)]) => {
					let max_bytes = parse_number_literal(n.value())? as u32;
					Some(Constraint::MaxBytes(max_bytes.into()))
				}
				(ValueType::Uint, [AstLiteral::Number(n)]) => {
					let max_bytes = parse_number_literal(n.value())? as u32;
					Some(Constraint::MaxBytes(max_bytes.into()))
				}
				(ValueType::Decimal, [AstLiteral::Number(p), AstLiteral::Number(s)]) => {
					let precision = parse_number_literal(p.value())? as u8;
					let scale = parse_number_literal(s.value())? as u8;
					Some(Constraint::PrecisionScale(precision.into(), scale.into()))
				}
				(ValueType::Vector(_), [AstLiteral::Number(n)]) => {
					let dims = parse_number_literal(n.value())? as u32;
					Some(Constraint::Dimension(Dimension::try_new(dims)?))
				}

				_ => {
					// A parenthesized type whose parameters match no known shape (decimal(5),
					// utf8(1, 2), ...) must be rejected rather than silently unconstrained.
					if !params.is_empty() {
						return Err(AstError::TypeParameterMismatch {
							type_name: name.text().to_string(),
							fragment: name.to_owned(),
						}
						.into());
					}
					None
				}
			};

			match constraint {
				Some(c) => Ok(TypeConstraint::with_constraint(base_type, c)),
				None => Ok(TypeConstraint::unconstrained(base_type)),
			}
		}
		AstType::Optional(inner) => {
			let inner_tc = convert_data_type_with_constraints(inner)?;
			let base_type = ValueType::Option(Box::new(inner_tc.get_type()));
			// The constraint has to survive the Option wrapper: a column type is persisted as a tag
			// byte plus a constraint, and the tag cannot carry a parameter. Dropping it here loses a
			// vector's dimension entirely and silently stops enforcing every other constraint.
			Ok(match inner_tc.constraint() {
				Some(constraint) => TypeConstraint::with_constraint(base_type, constraint.clone()),
				None => TypeConstraint::unconstrained(base_type),
			})
		}
		AstType::Qualified {
			name,
			..
		} => Err(AstError::UnrecognizedType {
			fragment: name.to_owned(),
		}
		.into()),
	}
}

fn parse_number_literal(s: &str) -> Result<usize> {
	s.parse::<usize>().map_err(|_| internal_error!("Invalid number literal: {}", s))
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
