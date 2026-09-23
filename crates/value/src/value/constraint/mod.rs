// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::{
	error::{ConstraintKind, Error, TypeError},
	fragment::Fragment,
	value::{
		Value, constraint::bytes::MaxBytes, dictionary::DictionaryId, sumtype::SumTypeId, value_type::ValueType,
	},
};

pub mod bytes;
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

	Dictionary(DictionaryId, ValueType),

	SumType(SumTypeId),
}

impl TypeConstraint {
	pub const fn unconstrained(ty: ValueType) -> Self {
		Self {
			base_type: ty,
			constraint: None,
		}
	}

	pub fn with_constraint(ty: ValueType, constraint: Constraint) -> Self {
		Self {
			base_type: ty,
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

	pub fn coerce(&self, value: &mut Value) -> Result<(), Error> {
		let value_type = value.get_type();
		if !matches!(value, Value::None { .. }) && !same_family(&value_type, self.base_type.inner_type()) {
			unimplemented!()
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
			(
				ValueType::Int {
					precision,
				},
				_,
			) => {
				if let Value::Int(int) = value
					&& int.digits() > precision.value()
				{
					return Err(precision_violation(
						ConstraintKind::IntPrecision {
							actual: int.digits(),
							max: precision.value(),
						},
						"INT",
						int.digits(),
						precision.value(),
					));
				}
			}
			(
				ValueType::Uint {
					precision,
				},
				_,
			) => {
				if let Value::Uint(uint) = value
					&& uint.digits() > precision.value()
				{
					return Err(precision_violation(
						ConstraintKind::UintPrecision {
							actual: uint.digits(),
							max: precision.value(),
						},
						"UINT",
						uint.digits(),
						precision.value(),
					));
				}
			}
			(
				ValueType::Decimal {
					precision,
					scale,
				},
				_,
			) => {
				if let Value::Decimal(decimal) = value {
					if decimal.scale() > scale.value() && decimal.rescale(scale.value()).is_none() {
						return Err(TypeError::ConstraintViolation {
							kind: ConstraintKind::DecimalScale {
								actual: decimal.scale(),
								max: scale.value(),
							},
							message: format!(
								"DECIMAL value {} has more fraction digits than the column scale {}",
								decimal,
								scale.value()
							),
							fragment: Fragment::None,
						}
						.into());
					}
					let digits = decimal.digits().saturating_sub(decimal.scale()) + scale.value();
					let rescaled = match decimal.fits(precision.value(), scale.value()) {
						Some(rescaled) => rescaled,
						None => {
							return Err(precision_violation(
								ConstraintKind::DecimalPrecision {
									actual: digits,
									max: precision.value(),
								},
								"DECIMAL",
								digits,
								precision.value(),
							));
						}
					};
					*decimal = rescaled;
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
			Some(Constraint::Dictionary(dict_id, id_type)) => {
				format!("DictionaryId(dict={}, {})", dict_id, id_type)
			}
			Some(Constraint::SumType(id)) => {
				format!("SumType({})", id)
			}
		}
	}
}

fn same_family(value_type: &ValueType, column_type: &ValueType) -> bool {
	match (value_type, column_type) {
		(
			ValueType::Int {
				..
			},
			ValueType::Int {
				..
			},
		)
		| (
			ValueType::Uint {
				..
			},
			ValueType::Uint {
				..
			},
		)
		| (
			ValueType::Decimal {
				..
			},
			ValueType::Decimal {
				..
			},
		) => true,
		_ => value_type == column_type,
	}
}

fn precision_violation(kind: ConstraintKind, name: &str, actual: u8, max: u8) -> Error {
	TypeError::ConstraintViolation {
		kind,
		message: format!("{} value exceeds maximum precision: {} digits (max: {} digits)", name, actual, max),
		fragment: Fragment::None,
	}
	.into()
}

#[cfg(test)]
pub mod tests {
	use std::str::FromStr;

	use super::*;
	use crate::value::{
		constraint::{precision::Precision, scale::Scale},
		decimal::Decimal,
		int::Int,
		uint::Uint,
	};

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
		let tc = TypeConstraint::unconstrained(decimal_10_2());
		assert_eq!(tc.base_type, ValueType::decimal(Precision::new(10), Scale::new(2)));
		assert_eq!(tc.base_type.precision(), Some(Precision::new(10)));
		assert_eq!(tc.base_type.scale(), Some(Scale::new(2)));
		assert_eq!(tc.constraint, None);
	}

	#[test]
	fn test_validate_utf8_within_limit() {
		let tc = TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(10)));
		let mut value = Value::Utf8("hello".to_string());
		assert!(tc.coerce(&mut value).is_ok());
	}

	#[test]
	fn test_validate_utf8_exceeds_limit() {
		let tc = TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(5)));
		let mut value = Value::Utf8("hello world".to_string());
		assert!(tc.coerce(&mut value).is_err());
	}

	#[test]
	fn test_validate_unconstrained() {
		let tc = TypeConstraint::unconstrained(ValueType::Utf8);
		let mut value = Value::Utf8("any length string is fine here".to_string());
		assert!(tc.coerce(&mut value).is_ok());
	}

	#[test]
	fn test_validate_none_rejected_for_non_option() {
		let tc = TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(5)));
		let mut value = Value::none();
		assert!(tc.coerce(&mut value).is_err());
	}

	#[test]
	fn test_validate_none_accepted_for_option() {
		let tc = TypeConstraint::unconstrained(ValueType::Option(Box::new(ValueType::Utf8)));
		let mut value = Value::none();
		assert!(tc.coerce(&mut value).is_ok());
	}

	#[test]
	fn test_to_string() {
		let tc1 = TypeConstraint::unconstrained(ValueType::Utf8);
		assert_eq!(tc1.to_string(), "Utf8");

		let tc2 = TypeConstraint::with_constraint(ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(50)));
		assert_eq!(tc2.to_string(), "Utf8(50)");

		let tc3 = TypeConstraint::unconstrained(decimal_10_2());
		assert_eq!(tc3.to_string(), "Decimal(10, 2)");
	}

	fn decimal_10_2() -> ValueType {
		ValueType::decimal(Precision::new(10), Scale::new(2))
	}

	fn coerce_decimal(column: ValueType, text: &str) -> Result<Value, Error> {
		let mut value = Value::Decimal(Decimal::from_str(text).unwrap());
		TypeConstraint::unconstrained(column).coerce(&mut value)?;
		Ok(value)
	}

	#[test]
	fn a_decimal_write_is_rescaled_to_the_column_scale_never_rounded() {
		// Rounding on write would store 1.24 for 1.235 and silently change what the user wrote.
		assert_eq!(coerce_decimal(decimal_10_2(), "1.2").unwrap().to_string(), "1.20");
		assert_eq!(coerce_decimal(decimal_10_2(), "1.23").unwrap().to_string(), "1.23");
		assert_eq!(coerce_decimal(decimal_10_2(), "1.2300").unwrap().to_string(), "1.23");
		assert_eq!(coerce_decimal(decimal_10_2(), "1.235").unwrap_err().code, "CONSTRAINT_006");
		assert_eq!(coerce_decimal(decimal_10_2(), "-0.001").unwrap_err().code, "CONSTRAINT_006");
		let optional = ValueType::Option(Box::new(decimal_10_2()));
		assert_eq!(coerce_decimal(optional.clone(), "7").unwrap().to_string(), "7.00");
		assert_eq!(coerce_decimal(optional, "7.001").unwrap_err().code, "CONSTRAINT_006");
	}

	#[test]
	fn a_decimal_write_counts_precision_at_the_column_scale() {
		// 12345678.9 has 9 digits but 10 at scale 2, so a precision check on the raw value lets it through
		// wrongly.
		assert_eq!(coerce_decimal(decimal_10_2(), "12345678.99").unwrap().to_string(), "12345678.99");
		assert_eq!(coerce_decimal(decimal_10_2(), "123456789.9").unwrap_err().code, "CONSTRAINT_005");
		assert_eq!(coerce_decimal(decimal_10_2(), "123456789").unwrap_err().code, "CONSTRAINT_005");
		let widest = ValueType::decimal(Precision::MAX, Scale::new(10));
		assert_eq!(coerce_decimal(widest, &"9".repeat(67)).unwrap_err().code, "CONSTRAINT_005");
	}

	#[test]
	fn int_and_uint_writes_are_bounded_by_the_declared_precision() {
		// Precision replaces the old byte limit, so a value with one digit too many must be refused.
		let int3 = TypeConstraint::unconstrained(ValueType::int(Precision::new(3)));
		let mut fits = Value::Int(Int::from(-999));
		assert!(int3.coerce(&mut fits).is_ok());
		let mut too_wide = Value::Int(Int::from(-1000));
		assert_eq!(int3.coerce(&mut too_wide).unwrap_err().code, "CONSTRAINT_003");
		let uint3 = TypeConstraint::unconstrained(ValueType::uint(Precision::new(3)));
		let mut fits = Value::Uint(Uint::from(999u16));
		assert!(uint3.coerce(&mut fits).is_ok());
		let mut too_wide = Value::Uint(Uint::from(1000u16));
		assert_eq!(uint3.coerce(&mut too_wide).unwrap_err().code, "CONSTRAINT_004");
		let mut widest = Value::Int(Int::MAX);
		assert!(TypeConstraint::unconstrained(ValueType::INT).coerce(&mut widest).is_ok());
	}
}
