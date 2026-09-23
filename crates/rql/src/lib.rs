// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

use reifydb_core::internal_error;
use reifydb_value::{
	Result,
	value::{
		constraint::{Constraint, TypeConstraint, precision::Precision, scale::Scale},
		digest::{Digest, DigestError, literal::parse_accuracy},
		value_type::ValueType,
	},
};

use crate::{
	ast::ast::{AstLiteral, AstType, AstTypeParameter},
	bump::BumpFragment,
	diagnostic::AstError,
};

pub mod ast;
pub mod bump;
pub mod compiler;
pub mod diagnostic;
pub(crate) mod duration;
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
		"int" => ValueType::INT,
		"uint" => ValueType::UINT,
		"decimal" => ValueType::DECIMAL,
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
		AstType::Unconstrained(name) if is_digest(name) => Err(AstError::DigestAccuracyMissing {
			fragment: name.to_owned(),
		}
		.into()),
		AstType::Unconstrained(name) => {
			let base_type = convert_data_type(name)?;
			Ok(TypeConstraint::unconstrained(base_type))
		}
		AstType::Constrained {
			name,
			params,
		} if is_digest(name) => Ok(TypeConstraint::unconstrained(convert_digest_type(name, params)?)),
		AstType::Constrained {
			name,
			params,
		} => {
			let base_type = convert_data_type(name)?;

			Ok(match (base_type.clone(), params.as_slice()) {
				(ValueType::Utf8, [AstTypeParameter::Literal(AstLiteral::Number(n))])
				| (ValueType::Blob, [AstTypeParameter::Literal(AstLiteral::Number(n))]) => {
					let max_bytes = parse_number_literal(n.value())? as u32;
					TypeConstraint::with_constraint(
						base_type,
						Constraint::MaxBytes(max_bytes.into()),
					)
				}
				(
					ValueType::Int {
						..
					},
					[AstTypeParameter::Literal(AstLiteral::Number(n))],
				) => TypeConstraint::unconstrained(ValueType::int(parse_precision(n.value())?)),
				(
					ValueType::Uint {
						..
					},
					[AstTypeParameter::Literal(AstLiteral::Number(n))],
				) => TypeConstraint::unconstrained(ValueType::uint(parse_precision(n.value())?)),
				(
					ValueType::Decimal {
						..
					},
					[
						AstTypeParameter::Literal(AstLiteral::Number(p)),
						AstTypeParameter::Literal(AstLiteral::Number(s)),
					],
				) => {
					let precision = parse_precision(p.value())?;
					let scale = Scale::try_new_with_precision(
						parse_type_parameter(s.value())?,
						precision,
					)?;
					TypeConstraint::unconstrained(ValueType::decimal(precision, scale))
				}
				_ => {
					return Err(AstError::UnsupportedTypeParameters {
						fragment: name.to_owned(),
					}
					.into());
				}
			})
		}
		AstType::Optional(inner) => {
			if let AstType::Optional(nested) = inner.as_ref() {
				return Err(AstError::NestedOption {
					fragment: nested.name_fragment().to_owned(),
				}
				.into());
			}
			let inner_tc = convert_data_type_with_constraints(inner)?;
			let base_type = ValueType::Option(Box::new(inner_tc.get_type()));
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

fn parse_type_parameter(s: &str) -> Result<u8> {
	Ok(u8::try_from(parse_number_literal(s)?).unwrap_or(u8::MAX))
}

fn parse_precision(s: &str) -> Result<Precision> {
	Ok(Precision::try_new(parse_type_parameter(s)?)?)
}

pub(crate) fn convert_procedure_param_type(ast: &AstType) -> Result<TypeConstraint> {
	match ast {
		AstType::Unconstrained(name) if is_list(name) => Err(AstError::ListItemMissing {
			fragment: name.to_owned(),
		}
		.into()),
		AstType::Constrained {
			name,
			params,
		} if is_list(name) => Ok(TypeConstraint::unconstrained(convert_list_type(name, params)?)),
		_ => convert_data_type_with_constraints(ast),
	}
}

fn is_list(name: &BumpFragment<'_>) -> bool {
	name.text().eq_ignore_ascii_case("list")
}

fn convert_list_type(name: &BumpFragment<'_>, params: &[AstTypeParameter<'_>]) -> Result<ValueType> {
	let item = match params {
		[] => {
			return Err(AstError::ListItemMissing {
				fragment: name.to_owned(),
			}
			.into());
		}
		[item] => item,
		[_, extra, ..] => {
			return Err(AstError::ListTooManyParameters {
				fragment: extra.fragment().to_owned(),
			}
			.into());
		}
	};

	let AstTypeParameter::Type(item_type) = item else {
		return Err(AstError::ListItemNotAType {
			fragment: item.fragment().to_owned(),
		}
		.into());
	};
	let item_constraint = convert_procedure_param_type(item_type)?;
	if item_constraint.constraint().is_some() {
		return Err(AstError::ListItemConstrained {
			fragment: item_type.name_fragment().to_owned(),
		}
		.into());
	}

	let item = item_constraint.get_type();
	if !item.is_scalar() {
		return Err(AstError::ListItemNotScalar {
			fragment: item_type.name_fragment().to_owned(),
		}
		.into());
	}
	Ok(ValueType::List(Box::new(item)))
}

fn is_digest(name: &BumpFragment<'_>) -> bool {
	name.text().eq_ignore_ascii_case("digest")
}

fn convert_digest_type(name: &BumpFragment<'_>, params: &[AstTypeParameter<'_>]) -> Result<ValueType> {
	let (input, accuracy) = match params {
		[] => {
			return Err(AstError::DigestAccuracyMissing {
				fragment: name.to_owned(),
			}
			.into());
		}
		[input] => {
			return Err(AstError::DigestAccuracyMissing {
				fragment: input.fragment().to_owned(),
			}
			.into());
		}
		[input, accuracy] => (input, accuracy),
		[_, _, extra, ..] => {
			return Err(AstError::DigestTooManyParameters {
				fragment: extra.fragment().to_owned(),
			}
			.into());
		}
	};

	let AstTypeParameter::Type(input_type) = input else {
		return Err(AstError::DigestInputNotAType {
			fragment: input.fragment().to_owned(),
		}
		.into());
	};
	let input_constraint = convert_data_type_with_constraints(input_type)?;
	if input_constraint.constraint().is_some() {
		return Err(AstError::UnsupportedTypeParameters {
			fragment: input_type.name_fragment().to_owned(),
		}
		.into());
	}

	let accuracy_fragment = accuracy.fragment().to_owned();
	let accuracy = match accuracy {
		AstTypeParameter::Literal(AstLiteral::Number(number)) => parse_accuracy(number.value()),
		_ => Err(DigestError::AccuracyNotANumber),
	}
	.map_err(|failure| match failure {
		DigestError::AccuracyOutOfRange => AstError::DigestAccuracyOutOfRange {
			fragment: accuracy_fragment,
		}
		.into(),
		DigestError::AccuracyNotWholePpm => AstError::DigestAccuracyNotWholePpm {
			fragment: accuracy_fragment,
		}
		.into(),
		DigestError::AccuracyNotANumber => AstError::DigestAccuracyNotANumber {
			fragment: accuracy_fragment,
		}
		.into(),
		other => internal_error!("digest accuracy parse failed: {}", other),
	})?;

	let inner = input_constraint.get_type();
	Digest::new(inner.clone(), accuracy).map_err(|failure| match failure {
		DigestError::UnsupportedInnerType {
			inner,
		} => AstError::DigestInputTypeUnsupported {
			inner,
			fragment: input_type.name_fragment().to_owned(),
		}
		.into(),
		other => internal_error!("digest type check failed: {}", other),
	})?;

	Ok(ValueType::Digest {
		inner: Box::new(inner),
		accuracy,
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
