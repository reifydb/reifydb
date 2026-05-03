// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
use reifydb_type::{
	Result,
	value::{
		constraint::{Constraint, TypeConstraint},
		r#type::Type,
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
pub mod explain;
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

pub(crate) fn convert_data_type(ast: &BumpFragment<'_>) -> Result<Type> {
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
		"identityid" | "identity_id" => Type::IdentityId,
		"blob" => Type::Blob,
		"int" => Type::Int,
		"uint" => Type::Uint,
		"decimal" => Type::Decimal,
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
			Ok(TypeConstraint::unconstrained(base_type))
		}
		AstType::Constrained {
			name,
			params,
		} => {
			let base_type = convert_data_type(name)?;

			let constraint = match (base_type.clone(), params.as_slice()) {
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

				_ => None,
			};

			match constraint {
				Some(c) => Ok(TypeConstraint::with_constraint(base_type, c)),
				None => Ok(TypeConstraint::unconstrained(base_type)),
			}
		}
		AstType::Optional(inner) => {
			let inner_tc = convert_data_type_with_constraints(inner)?;
			Ok(TypeConstraint::unconstrained(Type::Option(Box::new(inner_tc.get_type()))))
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
