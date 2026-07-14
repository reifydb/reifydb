// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::{
	error::{ConstraintKind, Error, TypeError},
	fragment::Fragment,
	value::{
		Value,
		constraint::{bytes::MaxBytes, dimension::Dimension, precision::Precision, scale::Scale},
		dictionary::DictionaryId,
		sumtype::SumTypeId,
		value_type::ValueType,
	},
};

pub mod bytes;
pub mod dimension;
pub mod precision;
pub mod scale;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeConstraint {
	base_type: ValueType,
	constraint: Option<Constraint>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Constraint {
	MaxBytes(MaxBytes),

	PrecisionScale(Precision, Scale),

	Dictionary(DictionaryId, ValueType),

	SumType(SumTypeId),

	Dimension(Dimension),
}

impl TypeConstraint {
	pub const fn unconstrained(ty: ValueType) -> Self {
		Self {
			base_type: ty,
			constraint: None,
		}
	}

	pub fn with_constraint(ty: ValueType, constraint: Constraint) -> Self {
		// A type tag byte cannot carry a vector's dimension, so it travels in the constraint slot.
		// Rebuilding the parameterized base type here means every decode path (catalog, FFI, codec)
		// gets Vector(dims) back without knowing about it.
		let base_type = match (&ty, &constraint) {
			(ValueType::Vector(_), Constraint::Dimension(dims)) => ValueType::Vector(dims.value()),
			(ValueType::Option(inner), Constraint::Dimension(dims))
				if matches!(**inner, ValueType::Vector(_)) =>
			{
				ValueType::Option(Box::new(ValueType::Vector(dims.value())))
			}
			_ => ty,
		};
		Self {
			base_type,
			constraint: Some(constraint),
		}
	}

	pub fn dictionary(dictionary_id: DictionaryId, id_type: ValueType) -> Self {
		Self {
			base_type: ValueType::DictionaryId,
			constraint: Some(Constraint::Dictionary(dictionary_id, id_type)),
		}
	}

	pub fn sumtype(id: SumTypeId) -> Self {
		Self {
			base_type: ValueType::Uint1,
			constraint: Some(Constraint::SumType(id)),
		}
	}

	pub fn vector(dims: Dimension) -> Self {
		Self {
			base_type: ValueType::Vector(dims.value()),
			constraint: Some(Constraint::Dimension(dims)),
		}
	}

	pub fn get_type(&self) -> ValueType {
		self.base_type.clone()
	}

	pub fn storage_type(&self) -> ValueType {
		match (&self.base_type, &self.constraint) {
			(ValueType::DictionaryId, Some(Constraint::Dictionary(_, id_type))) => id_type.clone(),
			_ => self.base_type.clone(),
		}
	}

	pub fn constraint(&self) -> &Option<Constraint> {
		&self.constraint
	}

	pub fn validate(&self, value: &Value) -> Result<(), Error> {
		if let (ValueType::Vector(expected), Value::Vector(vector)) = (self.base_type.inner_type(), value) {
			let actual = vector.dims();
			if actual != *expected as usize {
				return Err(TypeError::ConstraintViolation {
					kind: ConstraintKind::VectorDimension {
						actual,
						expected: *expected as usize,
					},
					message: format!(
						"VECTOR value has {} dimensions (column requires {})",
						actual, expected
					),
					fragment: Fragment::None,
				}
				.into());
			}
		}

		let value_type = value.get_type();
		if value_type != self.base_type && !matches!(value, Value::None { .. }) {
			if let ValueType::Option(inner) = &self.base_type {
				if value_type != **inner {
					unimplemented!()
				}
			} else {
				unimplemented!()
			}
		}

		if matches!(value, Value::None { .. }) {
			if self.base_type.is_option() {
				return Ok(());
			} else {
				return Err(TypeError::ConstraintViolation {
					kind: ConstraintKind::NoneNotAllowed {
						column_type: self.base_type.clone(),
					},
					message: format!(
						"Cannot insert none into non-optional column of type {}. Declare the column as Option({}) to allow none values.",
						self.base_type, self.base_type
					),
					fragment: Fragment::None,
				}
				.into());
			}
		}

		match (self.base_type.inner_type(), &self.constraint) {
			(ValueType::Utf8, Some(Constraint::MaxBytes(max))) => {
				if let Value::Utf8(s) = value {
					let byte_len = s.len();
					let max_value: usize = (*max).into();
					if byte_len > max_value {
						return Err(TypeError::ConstraintViolation {
							kind: ConstraintKind::Utf8MaxBytes {
								actual: byte_len,
								max: max_value,
							},
							message: format!(
								"UTF8 value exceeds maximum byte length: {} bytes (max: {} bytes)",
								byte_len, max_value
							),
							fragment: Fragment::None,
						}
						.into());
					}
				}
			}
			(ValueType::Blob, Some(Constraint::MaxBytes(max))) => {
				if let Value::Blob(blob) = value {
					let byte_len = blob.len();
					let max_value: usize = (*max).into();
					if byte_len > max_value {
						return Err(TypeError::ConstraintViolation {
							kind: ConstraintKind::BlobMaxBytes {
								actual: byte_len,
								max: max_value,
							},
							message: format!(
								"BLOB value exceeds maximum byte length: {} bytes (max: {} bytes)",
								byte_len, max_value
							),
							fragment: Fragment::None,
						}
						.into());
					}
				}
			}
			(ValueType::Int, Some(Constraint::MaxBytes(max))) => {
				if let Value::Int(vi) = value {
					let str_len = vi.to_string().len();
					let byte_len = (str_len * 415 / 1000) + 1;
					let max_value: usize = (*max).into();
					if byte_len > max_value {
						return Err(TypeError::ConstraintViolation {
							kind: ConstraintKind::IntMaxBytes {
								actual: byte_len,
								max: max_value,
							},
							message: format!(
								"INT value exceeds maximum byte length: {} bytes (max: {} bytes)",
								byte_len, max_value
							),
							fragment: Fragment::None,
						}
						.into());
					}
				}
			}
			(ValueType::Uint, Some(Constraint::MaxBytes(max))) => {
				if let Value::Uint(vu) = value {
					let str_len = vu.to_string().len();
					let byte_len = (str_len * 415 / 1000) + 1;
					let max_value: usize = (*max).into();
					if byte_len > max_value {
						return Err(TypeError::ConstraintViolation {
							kind: ConstraintKind::UintMaxBytes {
								actual: byte_len,
								max: max_value,
							},
							message: format!(
								"UINT value exceeds maximum byte length: {} bytes (max: {} bytes)",
								byte_len, max_value
							),
							fragment: Fragment::None,
						}
						.into());
					}
				}
			}
			(ValueType::Decimal, Some(Constraint::PrecisionScale(precision, scale))) => {
				if let Value::Decimal(decimal) = value {
					let decimal_str = decimal.to_string();

					let decimal_scale: u8 = if let Some(dot_pos) = decimal_str.find('.') {
						let after_dot = &decimal_str[dot_pos + 1..];
						after_dot.len().min(255) as u8
					} else {
						0
					};

					let decimal_precision: u8 =
						decimal_str.chars().filter(|c| c.is_ascii_digit()).count().min(255)
							as u8;

					let scale_value: u8 = (*scale).into();
					let precision_value: u8 = (*precision).into();

					if decimal_scale > scale_value {
						return Err(TypeError::ConstraintViolation {
							kind: ConstraintKind::DecimalScale {
								actual: decimal_scale,
								max: scale_value,
							},
							message: format!(
								"DECIMAL value exceeds maximum scale: {} decimal places (max: {} decimal places)",
								decimal_scale, scale_value
							),
							fragment: Fragment::None,
						}
						.into());
					}
					if decimal_precision > precision_value {
						return Err(TypeError::ConstraintViolation {
							kind: ConstraintKind::DecimalPrecision {
								actual: decimal_precision,
								max: precision_value,
							},
							message: format!(
								"DECIMAL value exceeds maximum precision: {} digits (max: {} digits)",
								decimal_precision, precision_value
							),
							fragment: Fragment::None,
						}
						.into());
					}
				}
			}
			(ValueType::Vector(_), _) => {
				if let Value::Vector(vector) = value
					&& let Some(index) = vector.as_slice().iter().position(|v| !v.is_finite())
				{
					return Err(TypeError::ConstraintViolation {
						kind: ConstraintKind::VectorNotFinite {
							index,
						},
						message: format!(
							"VECTOR value has a non-finite element at index {}; NaN and infinity are not allowed",
							index
						),
						fragment: Fragment::None,
					}
					.into());
				}
			}

			_ => {}
		}

		Ok(())
	}

	pub fn is_unconstrained(&self) -> bool {
		self.constraint.is_none()
	}

	#[allow(clippy::inherent_to_string)]
	pub fn to_string(&self) -> String {
		match &self.constraint {
			None => format!("{}", self.base_type),
			Some(Constraint::MaxBytes(max)) => {
				format!("{}({})", self.base_type, max)
			}
			Some(Constraint::PrecisionScale(p, s)) => {
				format!("{}({},{})", self.base_type, p, s)
			}
			Some(Constraint::Dictionary(dict_id, id_type)) => {
				format!("DictionaryId(dict={}, {})", dict_id, id_type)
			}
			Some(Constraint::SumType(id)) => {
				format!("SumType({})", id)
			}
			Some(Constraint::Dimension(_)) => {
				format!("{}", self.base_type)
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_unconstrained_type() {
		let tc = TypeConstraint::unconstrained(ValueType::Utf8);
		assert_eq!(tc.base_type, ValueType::Utf8);
		assert_eq!(tc.constraint, None);
		assert!(tc.is_unconstrained());
	}

	#[test]
	fn test_constrained_utf8() {
		let tc = TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(50)));
		assert_eq!(tc.base_type, ValueType::Utf8);
		assert_eq!(tc.constraint, Some(Constraint::MaxBytes(MaxBytes::new(50))));
		assert!(!tc.is_unconstrained());
	}

	#[test]
	fn test_constrained_decimal() {
		let tc = TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
		);
		assert_eq!(tc.base_type, ValueType::Decimal);
		assert_eq!(tc.constraint, Some(Constraint::PrecisionScale(Precision::new(10), Scale::new(2))));
	}

	#[test]
	fn test_validate_utf8_within_limit() {
		let tc = TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(10)));
		let value = Value::Utf8("hello".to_string());
		assert!(tc.validate(&value).is_ok());
	}

	#[test]
	fn test_validate_utf8_exceeds_limit() {
		let tc = TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(5)));
		let value = Value::Utf8("hello world".to_string());
		assert!(tc.validate(&value).is_err());
	}

	#[test]
	fn test_validate_unconstrained() {
		let tc = TypeConstraint::unconstrained(ValueType::Utf8);
		let value = Value::Utf8("any length string is fine here".to_string());
		assert!(tc.validate(&value).is_ok());
	}

	#[test]
	fn test_vector_pins_base_type() {
		let tc = TypeConstraint::vector(Dimension::new(768));
		assert_eq!(tc.base_type, ValueType::Vector(768));
		assert_eq!(tc.constraint, Some(Constraint::Dimension(Dimension::new(768))));
	}

	#[test]
	fn test_validate_vector_matching_dimension() {
		let tc = TypeConstraint::vector(Dimension::new(4));
		let value = Value::vector(vec![0.1, 0.2, 0.3, 0.4]);
		assert!(tc.validate(&value).is_ok());
	}

	#[test]
	fn test_validate_vector_wrong_dimension_is_rejected() {
		let tc = TypeConstraint::vector(Dimension::new(4));
		let value = Value::vector(vec![0.1, 0.2, 0.3]);

		let err = tc.validate(&value).unwrap_err();
		let diagnostic = err.0;
		assert_eq!(diagnostic.code, "CONSTRAINT_008");
		assert!(
			diagnostic.message.contains("3 dimensions") && diagnostic.message.contains("requires 4"),
			"unexpected message: {}",
			diagnostic.message
		);
	}

	#[test]
	fn test_validate_vector_rejects_none_in_non_optional_column() {
		let tc = TypeConstraint::vector(Dimension::new(4));
		assert!(tc.validate(&Value::none_of(ValueType::Vector(4))).is_err());
	}

	#[test]
	fn test_to_string_renders_vector_dimension() {
		assert_eq!(TypeConstraint::vector(Dimension::new(768)).to_string(), "Vector(768)");
	}

	// A non-finite element makes every distance computed against the vector non-finite, which
	// does not sort meaningfully. Rejecting it at insert is what keeps nearest-neighbour ordering
	// well defined, so these cases must fail rather than store.

	#[test]
	fn test_validate_vector_rejects_nan_element() {
		let tc = TypeConstraint::vector(Dimension::new(3));
		let value = Value::vector(vec![0.1, f32::NAN, 0.3]);

		let err = tc.validate(&value).unwrap_err();
		let diagnostic = err.0;
		assert_eq!(diagnostic.code, "CONSTRAINT_009");
		assert!(
			diagnostic.message.contains("index 1"),
			"expected the offending index, got: {}",
			diagnostic.message
		);
	}

	#[test]
	fn test_validate_vector_rejects_infinite_element() {
		let tc = TypeConstraint::vector(Dimension::new(2));
		assert!(tc.validate(&Value::vector(vec![f32::INFINITY, 0.0])).is_err());
		assert!(tc.validate(&Value::vector(vec![0.0, f32::NEG_INFINITY])).is_err());
	}

	#[test]
	fn test_validate_vector_accepts_finite_extremes() {
		let tc = TypeConstraint::vector(Dimension::new(3));
		let value = Value::vector(vec![f32::MIN, -0.0, f32::MAX]);
		assert!(tc.validate(&value).is_ok());
	}

	#[test]
	fn test_validate_none_rejected_for_non_option() {
		let tc = TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(5)));
		let value = Value::none();
		assert!(tc.validate(&value).is_err());
	}

	#[test]
	fn test_validate_none_accepted_for_option() {
		let tc = TypeConstraint::unconstrained(ValueType::Option(Box::new(ValueType::Utf8)));
		let value = Value::none();
		assert!(tc.validate(&value).is_ok());
	}

	#[test]
	fn test_to_string() {
		let tc1 = TypeConstraint::unconstrained(ValueType::Utf8);
		assert_eq!(tc1.to_string(), "Utf8");

		let tc2 = TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(50)));
		assert_eq!(tc2.to_string(), "Utf8(50)");

		let tc3 = TypeConstraint::with_constraint(
			ValueType::Decimal,
			Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
		);
		assert_eq!(tc3.to_string(), "Decimal(10,2)");
	}
}
