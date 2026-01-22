// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::{
	error::{Error, diagnostic::constraint::utf8_exceeds_max_bytes},
	fragment::Fragment,
	value::{
		Value,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		r#type::Type,
	},
};

pub mod bytes;
pub mod precision;
pub mod scale;

/// Represents a type with optional constraints
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeConstraint {
	base_type: Type,
	constraint: Option<Constraint>,
}

/// Constraint types for different data types
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Constraint {
	/// Maximum number of bytes for UTF8, BLOB, INT, UINT
	MaxBytes(MaxBytes),
	/// Precision and scale for DECIMAL
	PrecisionScale(Precision, Scale),
}

/// FFI-safe representation of a TypeConstraint
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct FFITypeConstraint {
	/// Base type code (Type::to_u8)
	pub base_type: u8,
	/// Constraint type: 0=None, 1=MaxBytes, 2=PrecisionScale
	pub constraint_type: u8,
	/// First constraint param: MaxBytes value OR precision
	pub constraint_param1: u32,
	/// Second constraint param: scale (only for PrecisionScale)
	pub constraint_param2: u32,
}

impl TypeConstraint {
	/// Create an unconstrained type (const for use in static contexts)
	pub const fn unconstrained(ty: Type) -> Self {
		Self {
			base_type: ty,
			constraint: None,
		}
	}

	/// Create a type with a constraint
	pub fn with_constraint(ty: Type, constraint: Constraint) -> Self {
		Self {
			base_type: ty,
			constraint: Some(constraint),
		}
	}

	/// Get the base type
	pub fn get_type(&self) -> Type {
		self.base_type
	}

	/// Get the constraint
	pub fn constraint(&self) -> &Option<Constraint> {
		&self.constraint
	}

	/// Convert to FFI representation
	pub fn to_ffi(&self) -> FFITypeConstraint {
		let base_type = self.base_type.to_u8();
		match &self.constraint {
			None => FFITypeConstraint {
				base_type,
				constraint_type: 0,
				constraint_param1: 0,
				constraint_param2: 0,
			},
			Some(Constraint::MaxBytes(max)) => FFITypeConstraint {
				base_type,
				constraint_type: 1,
				constraint_param1: max.value(),
				constraint_param2: 0,
			},
			Some(Constraint::PrecisionScale(p, s)) => FFITypeConstraint {
				base_type,
				constraint_type: 2,
				constraint_param1: p.value() as u32,
				constraint_param2: s.value() as u32,
			},
		}
	}

	/// Create from FFI representation
	pub fn from_ffi(ffi: FFITypeConstraint) -> Self {
		let ty = Type::from_u8(ffi.base_type);
		match ffi.constraint_type {
			1 => Self::with_constraint(ty, Constraint::MaxBytes(MaxBytes::new(ffi.constraint_param1))),
			2 => Self::with_constraint(
				ty,
				Constraint::PrecisionScale(
					Precision::new(ffi.constraint_param1 as u8),
					Scale::new(ffi.constraint_param2 as u8),
				),
			),
			_ => Self::unconstrained(ty),
		}
	}

	/// Validate a value against this type constraint
	pub fn validate(&self, value: &Value) -> Result<(), Error> {
		// First check type compatibility
		let value_type = value.get_type();
		if value_type != self.base_type && value_type != Type::Undefined {
			// For now, return a simple error - we'll create proper
			// diagnostics later
			// return Err(crate::error!(crate::error::diagnostic::internal::internal(format!(
			// 	"Type mismatch: expected {}, got {}",
			// 	self.base_type, value_type
			// ))));
			unimplemented!()
		}

		// If undefined, no further validation needed
		if matches!(value, Value::Undefined) {
			return Ok(());
		}

		// Check constraints if present
		match (&self.base_type, &self.constraint) {
			(Type::Utf8, Some(Constraint::MaxBytes(max))) => {
				if let Value::Utf8(s) = value {
					let byte_len = s.as_bytes().len();
					let max_value: usize = (*max).into();
					if byte_len > max_value {
						return Err(crate::error!(utf8_exceeds_max_bytes(
							Fragment::None,
							byte_len,
							max_value
						)));
					}
				}
			}
			(Type::Blob, Some(Constraint::MaxBytes(max))) => {
				if let Value::Blob(blob) = value {
					let byte_len = blob.len();
					let max_value: usize = (*max).into();
					if byte_len > max_value {
						return Err(crate::error!(
							crate::error::diagnostic::constraint::blob_exceeds_max_bytes(
								Fragment::None,
								byte_len,
								max_value
							)
						));
					}
				}
			}
			(Type::Int, Some(Constraint::MaxBytes(max))) => {
				if let Value::Int(vi) = value {
					// Calculate byte size of Int by
					// converting to string and estimating
					// This is a rough estimate: each
					// decimal digit needs ~3.32 bits, so
					// ~0.415 bytes
					let str_len = vi.to_string().len();
					let byte_len = (str_len * 415 / 1000) + 1; // Rough estimate
					let max_value: usize = (*max).into();
					if byte_len > max_value {
						return Err(crate::error!(
							crate::error::diagnostic::constraint::int_exceeds_max_bytes(
								Fragment::None,
								byte_len,
								max_value
							)
						));
					}
				}
			}
			(Type::Uint, Some(Constraint::MaxBytes(max))) => {
				if let Value::Uint(vu) = value {
					// Calculate byte size of Uint by
					// converting to string and estimating
					// This is a rough estimate: each
					// decimal digit needs ~3.32 bits, so
					// ~0.415 bytes
					let str_len = vu.to_string().len();
					let byte_len = (str_len * 415 / 1000) + 1; // Rough estimate
					let max_value: usize = (*max).into();
					if byte_len > max_value {
						return Err(crate::error!(
							crate::error::diagnostic::constraint::uint_exceeds_max_bytes(
								Fragment::None,
								byte_len,
								max_value
							)
						));
					}
				}
			}
			(Type::Decimal, Some(Constraint::PrecisionScale(precision, scale))) => {
				if let Value::Decimal(decimal) = value {
					// Calculate precision and scale from
					// BigDecimal
					let decimal_str = decimal.to_string();

					// Calculate scale (digits after decimal
					// point)
					let decimal_scale: u8 = if let Some(dot_pos) = decimal_str.find('.') {
						let after_dot = &decimal_str[dot_pos + 1..];
						after_dot.len().min(255) as u8
					} else {
						0
					};

					// Calculate precision (total number of
					// significant digits)
					let decimal_precision: u8 =
						decimal_str.chars().filter(|c| c.is_ascii_digit()).count().min(255)
							as u8;

					let scale_value: u8 = (*scale).into();
					let precision_value: u8 = (*precision).into();

					if decimal_scale > scale_value {
						return Err(crate::error!(
							crate::error::diagnostic::constraint::decimal_exceeds_scale(
								Fragment::None,
								decimal_scale,
								scale_value
							)
						));
					}
					if decimal_precision > precision_value {
						return Err(crate::error!(
							crate::error::diagnostic::constraint::decimal_exceeds_precision(
								Fragment::None,
								decimal_precision,
								precision_value
							)
						));
					}
				}
			}
			// No constraint or non-applicable constraint
			_ => {}
		}

		Ok(())
	}

	/// Check if this type is unconstrained
	pub fn is_unconstrained(&self) -> bool {
		self.constraint.is_none()
	}

	/// Get a human-readable string representation
	pub fn to_string(&self) -> String {
		match &self.constraint {
			None => format!("{}", self.base_type),
			Some(Constraint::MaxBytes(max)) => {
				format!("{}({})", self.base_type, max)
			}
			Some(Constraint::PrecisionScale(p, s)) => {
				format!("{}({},{})", self.base_type, p, s)
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_unconstrained_type() {
		let tc = TypeConstraint::unconstrained(Type::Utf8);
		assert_eq!(tc.base_type, Type::Utf8);
		assert_eq!(tc.constraint, None);
		assert!(tc.is_unconstrained());
	}

	#[test]
	fn test_constrained_utf8() {
		let tc = TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(50)));
		assert_eq!(tc.base_type, Type::Utf8);
		assert_eq!(tc.constraint, Some(Constraint::MaxBytes(MaxBytes::new(50))));
		assert!(!tc.is_unconstrained());
	}

	#[test]
	fn test_constrained_decimal() {
		let tc = TypeConstraint::with_constraint(
			Type::Decimal,
			Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
		);
		assert_eq!(tc.base_type, Type::Decimal);
		assert_eq!(tc.constraint, Some(Constraint::PrecisionScale(Precision::new(10), Scale::new(2))));
	}

	#[test]
	fn test_validate_utf8_within_limit() {
		let tc = TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(10)));
		let value = Value::Utf8("hello".to_string());
		assert!(tc.validate(&value).is_ok());
	}

	#[test]
	fn test_validate_utf8_exceeds_limit() {
		let tc = TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(5)));
		let value = Value::Utf8("hello world".to_string());
		assert!(tc.validate(&value).is_err());
	}

	#[test]
	fn test_validate_unconstrained() {
		let tc = TypeConstraint::unconstrained(Type::Utf8);
		let value = Value::Utf8("any length string is fine here".to_string());
		assert!(tc.validate(&value).is_ok());
	}

	#[test]
	fn test_validate_undefined() {
		let tc = TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(5)));
		let value = Value::Undefined;
		assert!(tc.validate(&value).is_ok());
	}

	#[test]
	fn test_to_string() {
		let tc1 = TypeConstraint::unconstrained(Type::Utf8);
		assert_eq!(tc1.to_string(), "Utf8");

		let tc2 = TypeConstraint::with_constraint(Type::Utf8, Constraint::MaxBytes(MaxBytes::new(50)));
		assert_eq!(tc2.to_string(), "Utf8(50)");

		let tc3 = TypeConstraint::with_constraint(
			Type::Decimal,
			Constraint::PrecisionScale(Precision::new(10), Scale::new(2)),
		);
		assert_eq!(tc3.to_string(), "Decimal(10,2)");
	}
}
