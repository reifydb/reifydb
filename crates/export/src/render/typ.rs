// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	constraint::{Constraint, TypeConstraint},
	value_type::ValueType,
};

use crate::{error::ExportError, model::NameResolver};

pub struct RenderedColumnType {
	pub type_text: String,
	pub dictionary: Option<String>,
}

pub fn render_column_type(
	constraint: &TypeConstraint,
	resolver: &NameResolver,
	shape: &str,
) -> Result<RenderedColumnType, ExportError> {
	match constraint.constraint() {
		Some(Constraint::Dictionary(dict_id, _)) => {
			let id = dict_id.to_u64();
			let resolved = resolver.dictionary(id).ok_or_else(|| ExportError::UnresolvedReference {
				kind: "dictionary",
				id,
				shape: shape.to_string(),
			})?;
			Ok(RenderedColumnType {
				type_text: render_value_type(&resolved.value_type, shape)?,
				dictionary: Some(resolved.qualified_name.clone()),
			})
		}
		Some(Constraint::SumType(sum_id)) => {
			let id = sum_id.to_u64();
			let resolved = resolver.sumtype(id).ok_or_else(|| ExportError::UnresolvedReference {
				kind: "sumtype",
				id,
				shape: shape.to_string(),
			})?;
			Ok(RenderedColumnType {
				type_text: resolved.qualified_name.clone(),
				dictionary: None,
			})
		}
		Some(Constraint::MaxBytes(max)) => {
			let base = render_value_type(&constraint.get_type(), shape)?;
			Ok(RenderedColumnType {
				type_text: format!("{}({})", base, max),
				dictionary: None,
			})
		}
		Some(Constraint::PrecisionScale(precision, scale)) => Ok(RenderedColumnType {
			type_text: format!("decimal({},{})", precision, scale),
			dictionary: None,
		}),
		None => Ok(RenderedColumnType {
			type_text: render_value_type(&constraint.get_type(), shape)?,
			dictionary: None,
		}),
	}
}

pub fn render_value_type(ty: &ValueType, shape: &str) -> Result<String, ExportError> {
	let text = match ty {
		ValueType::Boolean => "bool".to_string(),
		ValueType::Float4 => "float4".to_string(),
		ValueType::Float8 => "float8".to_string(),
		ValueType::Int1 => "int1".to_string(),
		ValueType::Int2 => "int2".to_string(),
		ValueType::Int4 => "int4".to_string(),
		ValueType::Int8 => "int8".to_string(),
		ValueType::Int16 => "int16".to_string(),
		ValueType::Uint1 => "uint1".to_string(),
		ValueType::Uint2 => "uint2".to_string(),
		ValueType::Uint4 => "uint4".to_string(),
		ValueType::Uint8 => "uint8".to_string(),
		ValueType::Uint16 => "uint16".to_string(),
		ValueType::Utf8 => "utf8".to_string(),
		ValueType::Date => "date".to_string(),
		ValueType::DateTime => "datetime".to_string(),
		ValueType::Time => "time".to_string(),
		ValueType::Duration => "duration".to_string(),
		ValueType::IdentityId => "identityid".to_string(),
		ValueType::Uuid4 => "uuid4".to_string(),
		ValueType::Uuid7 => "uuid7".to_string(),
		ValueType::Blob => "blob".to_string(),
		ValueType::Int => "int".to_string(),
		ValueType::Uint => "uint".to_string(),
		ValueType::Decimal => "decimal".to_string(),
		ValueType::Option(inner) => format!("option({})", render_value_type(inner, shape)?),
		ValueType::Any
		| ValueType::DictionaryId
		| ValueType::List(_)
		| ValueType::Record(_)
		| ValueType::Tuple(_) => {
			return Err(ExportError::UnsupportedType {
				shape: shape.to_string(),
				value_type: format!("{}", ty),
			});
		}
	};
	Ok(text)
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::constraint::{bytes::MaxBytes, precision::Precision, scale::Scale};

	use super::*;

	#[test]
	fn plain_types_render_lowercase() {
		let r = NameResolver::empty();
		assert_eq!(
			render_column_type(&TypeConstraint::unconstrained(ValueType::Int4), &r, "s").unwrap().type_text,
			"int4"
		);
		assert_eq!(
			render_column_type(&TypeConstraint::unconstrained(ValueType::Utf8), &r, "s").unwrap().type_text,
			"utf8"
		);
	}

	#[test]
	fn option_wraps_inner() {
		let r = NameResolver::empty();
		let tc = TypeConstraint::unconstrained(ValueType::Option(Box::new(ValueType::Int4)));
		assert_eq!(render_column_type(&tc, &r, "s").unwrap().type_text, "option(int4)");
	}

	#[test]
	fn constrained_utf8_and_decimal() {
		let r = NameResolver::empty();
		let utf8 = TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(255)));
		assert_eq!(render_column_type(&utf8, &r, "s").unwrap().type_text, "utf8(255)");

		let dec = TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
		);
		assert_eq!(render_column_type(&dec, &r, "s").unwrap().type_text, "decimal(10,2)");
	}

	#[test]
	fn unsupported_base_type_fails_loud() {
		let r = NameResolver::empty();
		let tc = TypeConstraint::unconstrained(ValueType::Any);
		assert!(matches!(render_column_type(&tc, &r, "s"), Err(ExportError::UnsupportedType { .. })));
	}
}
