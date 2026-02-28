// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use super::{Diagnostic, IntoDiagnostic, util::value_range};
use crate::{
	error::{
		AstErrorKind, AuthErrorKind, BinaryOp, BlobEncodingKind, ConstraintKind, FunctionErrorKind, LogicalOp,
		NetworkErrorKind, OperandCategory, ProcedureErrorKind, RuntimeErrorKind, TemporalKind, TypeError,
	},
	fragment::Fragment,
	value::r#type::Type,
};

fn temporal_unit_name(unit: char) -> &'static str {
	match unit {
		'Y' => "year",
		'M' => "month/minute",
		'W' => "week",
		'D' => "day",
		'H' => "hour",
		'S' => "second",
		_ => "unit",
	}
}

impl IntoDiagnostic for TypeError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			TypeError::LogicalOperatorNotApplicable {
				operator,
				operand_category,
				fragment,
			} => {
				let code = match (&operator, &operand_category) {
					(LogicalOp::Not, OperandCategory::Number) => "OPERATOR_001",
					(LogicalOp::Not, OperandCategory::Text) => "OPERATOR_002",
					(LogicalOp::Not, OperandCategory::Temporal) => "OPERATOR_003",
					(LogicalOp::Not, OperandCategory::Uuid) => "OPERATOR_004",
					(LogicalOp::And, OperandCategory::Number) => "OPERATOR_005",
					(LogicalOp::And, OperandCategory::Text) => "OPERATOR_006",
					(LogicalOp::And, OperandCategory::Temporal) => "OPERATOR_007",
					(LogicalOp::And, OperandCategory::Uuid) => "OPERATOR_008",
					(LogicalOp::Or, OperandCategory::Number) => "OPERATOR_009",
					(LogicalOp::Or, OperandCategory::Text) => "OPERATOR_010",
					(LogicalOp::Or, OperandCategory::Temporal) => "OPERATOR_011",
					(LogicalOp::Or, OperandCategory::Uuid) => "OPERATOR_012",
					(LogicalOp::Xor, OperandCategory::Number) => "OPERATOR_013",
					(LogicalOp::Xor, OperandCategory::Text) => "OPERATOR_014",
					(LogicalOp::Xor, OperandCategory::Temporal) => "OPERATOR_015",
					(LogicalOp::Xor, OperandCategory::Uuid) => "OPERATOR_016",
				};

				let message = format!("Cannot apply {} operator to {}", operator, operand_category);
				let label = Some(format!("logical operator on {} type", match &operand_category {
					OperandCategory::Number => "numeric",
					OperandCategory::Text => "text",
					OperandCategory::Temporal => "temporal",
					OperandCategory::Uuid => "UUID",
				}));

				let help = format!(
					"The {} operator can only be applied to boolean values. Consider using comparison operators{}first",
					operator,
					match &operator {
						LogicalOp::Not => " or casting to boolean ",
						_ => " ",
					}
				);

				let mut notes = vec![];
				match &operator {
					LogicalOp::Not => {
						notes.push(
							"NOT is a logical operator that inverts boolean values (true becomes false, false becomes true)"
								.to_string(),
						);
					}
					LogicalOp::And => {
						notes.push("AND is a logical operator that combines boolean values".to_string());
					}
					LogicalOp::Or => {
						notes.push("OR is a logical operator that combines boolean values".to_string());
					}
					LogicalOp::Xor => {
						notes.push(
							"XOR is a logical operator that performs exclusive or on boolean values".to_string(),
						);
					}
				}

				match (&operator, &operand_category) {
					(LogicalOp::Not, OperandCategory::Number) => {
						notes.push("For numeric negation, use the minus (-) operator instead".to_string());
						notes.push("To convert numbers to boolean, use comparison operators like: value != 0".to_string());
					}
					(LogicalOp::Not, OperandCategory::Text) => {
						notes.push("To convert text to boolean, use comparison operators like: text != '...'".to_string());
						notes.push("For string operations, use appropriate string functions instead".to_string());
					}
					(LogicalOp::Not, OperandCategory::Temporal) => {
						notes.push("To convert temporal values to boolean, use comparison operators like: date > '2023-01-01'".to_string());
						notes.push("Temporal types include Date, DateTime, Time, and Duration".to_string());
					}
					(LogicalOp::Not, OperandCategory::Uuid) => {
						notes.push("To convert UUIDs to boolean, use comparison operators like: uuid == '...'".to_string());
						notes.push("UUID types include Uuid4 and Uuid7".to_string());
					}
					(_, OperandCategory::Number) => {
						notes.push("To convert numbers to boolean, use comparison operators like: value != 0".to_string());
						match &operator {
							LogicalOp::And => notes.push("For bitwise operations on integers, use the bitwise AND (&) operator instead".to_string()),
							LogicalOp::Or => notes.push("For bitwise operations on integers, use the bitwise OR (|) operator instead".to_string()),
							LogicalOp::Xor => notes.push("For bitwise operations on integers, use the bitwise XOR (^) operator instead".to_string()),
							_ => {}
						}
					}
					(_, OperandCategory::Text) => {
						notes.push("To convert text to boolean, use comparison operators like: text != '...'".to_string());
						match &operator {
							LogicalOp::And => notes.push("For text concatenation, use the string concatenation operator (||) instead".to_string()),
							LogicalOp::Or => notes.push("For text concatenation, use the string concatenation operator (+) instead".to_string()),
							LogicalOp::Xor => notes.push("XOR returns true when exactly one operand is true".to_string()),
							_ => {}
						}
					}
					(_, OperandCategory::Temporal) => {
						notes.push("To convert temporal values to boolean, use comparison operators like: date > '2023-01-01'".to_string());
						notes.push("Temporal types include Date, DateTime, Time, and Duration".to_string());
					}
					(_, OperandCategory::Uuid) => {
						notes.push("To convert UUIDs to boolean, use comparison operators like: uuid == '...'".to_string());
						if matches!(&operator, LogicalOp::Xor) {
							// no extra note
						}
						notes.push("UUID types include Uuid4 and Uuid7".to_string());
					}
				}

				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					column: None,
					fragment,
					label,
					help: Some(help),
					notes,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::BinaryOperatorNotApplicable {
				operator,
				left,
				right,
				fragment,
			} => {
				let code = match &operator {
					BinaryOp::Add => "OPERATOR_017",
					BinaryOp::Sub => "OPERATOR_018",
					BinaryOp::Mul => "OPERATOR_019",
					BinaryOp::Div => "OPERATOR_020",
					BinaryOp::Rem => "OPERATOR_021",
					BinaryOp::Equal => "OPERATOR_022",
					BinaryOp::NotEqual => "OPERATOR_023",
					BinaryOp::LessThan => "OPERATOR_024",
					BinaryOp::LessThanEqual => "OPERATOR_025",
					BinaryOp::GreaterThan => "OPERATOR_026",
					BinaryOp::GreaterThanEqual => "OPERATOR_027",
					BinaryOp::Between => "OPERATOR_028",
				};

				let sym = operator.symbol();
				let message = if matches!(&operator, BinaryOp::Between) {
					format!("Cannot apply '{}' operator to {} with range of {}", sym, left, right)
				} else {
					format!("Cannot apply '{}' operator to {} and {}", sym, left, right)
				};
				let label = Some(format!("'{}' operator on incompatible types", sym));

				let mut notes = if matches!(&operator, BinaryOp::Between) {
					vec![
						format!("Value is of type: {}", left),
						format!("Range bounds are of type: {}", right),
					]
				} else {
					vec![
						format!("Left operand is of type: {}", left),
						format!("Right operand is of type: {}", right),
					]
				};

				let comparison_note = match &operator {
					BinaryOp::Add
					| BinaryOp::Sub
					| BinaryOp::Mul
					| BinaryOp::Div
					| BinaryOp::Rem => {
						Some("Consider converting operands to compatible numeric types first".to_string())
					}
					BinaryOp::Equal => {
						Some("Equality comparison is only supported between compatible types".to_string())
					}
					BinaryOp::NotEqual => {
						Some("Inequality comparison is only supported between compatible types".to_string())
					}
					BinaryOp::LessThan => {
						Some("Less than comparison is only supported between compatible types".to_string())
					}
					BinaryOp::LessThanEqual => {
						Some("Less than or equal comparison is only supported between compatible types".to_string())
					}
					BinaryOp::GreaterThan => {
						Some("Greater than comparison is only supported between compatible types".to_string())
					}
					BinaryOp::GreaterThanEqual => {
						Some("Greater than or equal comparison is only supported between compatible types".to_string())
					}
					BinaryOp::Between => {
						Some("BETWEEN comparison is only supported between compatible types".to_string())
					}
				};
				if let Some(note) = comparison_note {
					notes.push(note);
				}

				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					column: None,
					fragment,
					label,
					help: None,
					notes,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::UnsupportedCast { from, to, fragment } => {
				let label = Some(format!("cannot cast {} of type {} to {}", fragment.text(), from, to));
				Diagnostic {
					code: "CAST_001".to_string(),
					statement: None,
					message: format!("unsupported cast from {} to {}", from, to),
					fragment,
					label,
					help: Some("ensure the source and target types are compatible for casting".to_string()),
					notes: vec![
						"supported casts include: numeric to numeric, string to temporal, boolean to numeric"
							.to_string(),
					],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::CastToNumberFailed {
				target,
				fragment,
				cause,
			} => {
				let label = Some(format!("failed to cast to {}", target));
				Diagnostic {
					code: "CAST_002".to_string(),
					statement: None,
					message: format!("failed to cast to {}", target),
					fragment,
					label,
					help: None,
					notes: vec![],
					column: None,
					cause: Some(Box::new((*cause).into_diagnostic())),
					operator_chain: None,
				}
			}

			TypeError::CastToTemporalFailed {
				target,
				fragment,
				cause,
			} => {
				let label = Some(format!("failed to cast to {}", target));
				Diagnostic {
					code: "CAST_003".to_string(),
					statement: None,
					message: format!("failed to cast to {}", target),
					fragment,
					label,
					help: None,
					notes: vec![],
					column: None,
					cause: Some(Box::new((*cause).into_diagnostic())),
					operator_chain: None,
				}
			}

			TypeError::CastToBooleanFailed { fragment, cause } => {
				let label = Some("failed to cast to bool".to_string());
				Diagnostic {
					code: "CAST_004".to_string(),
					statement: None,
					message: "failed to cast to bool".to_string(),
					fragment,
					label,
					help: None,
					notes: vec![],
					column: None,
					cause: Some(Box::new((*cause).into_diagnostic())),
					operator_chain: None,
				}
			}

			TypeError::CastToUuidFailed {
				target,
				fragment,
				cause,
			} => {
				let label = Some(format!("failed to cast to {}", target));
				Diagnostic {
					code: "CAST_005".to_string(),
					statement: None,
					message: format!("failed to cast to {}", target),
					fragment,
					label,
					help: None,
					notes: vec![],
					column: None,
					cause: Some(Box::new((*cause).into_diagnostic())),
					operator_chain: None,
				}
			}

			TypeError::CastBlobToUtf8Failed { fragment, cause } => {
				let label = Some("failed to cast BLOB to UTF8".to_string());
				Diagnostic {
					code: "CAST_006".to_string(),
					statement: None,
					message: "failed to cast BLOB to UTF8".to_string(),
					fragment,
					label,
					help: Some(
						"BLOB contains invalid UTF-8 bytes. Consider using to_utf8_lossy() function instead"
							.to_string(),
					),
					notes: vec![],
					column: None,
					cause: Some(Box::new((*cause).into_diagnostic())),
					operator_chain: None,
				}
			}

			TypeError::ConstraintViolation {
				kind,
				message,
				fragment,
			} => {
				let (code, help) = match &kind {
					ConstraintKind::Utf8MaxBytes { max, .. } => (
						"CONSTRAINT_001",
						format!(
							"The UTF8 field is constrained to a maximum of {} bytes. Consider shortening the text or increasing the constraint.",
							max
						),
					),
					ConstraintKind::BlobMaxBytes { max, .. } => (
						"CONSTRAINT_002",
						format!(
							"The BLOB field is constrained to a maximum of {} bytes. Consider reducing the data size or increasing the constraint.",
							max
						),
					),
					ConstraintKind::IntMaxBytes { max, .. } => (
						"CONSTRAINT_003",
						format!(
							"The INT field is constrained to a maximum of {} bytes. Consider using a smaller value or increasing the constraint.",
							max
						),
					),
					ConstraintKind::UintMaxBytes { max, .. } => (
						"CONSTRAINT_004",
						format!(
							"The UINT field is constrained to a maximum of {} bytes. Consider using a smaller value or increasing the constraint.",
							max
						),
					),
					ConstraintKind::DecimalPrecision { max, .. } => (
						"CONSTRAINT_005",
						format!(
							"The DECIMAL field is constrained to a maximum precision of {} digits. Consider using a smaller number or increasing the precision constraint.",
							max
						),
					),
					ConstraintKind::DecimalScale { max, .. } => (
						"CONSTRAINT_006",
						format!(
							"The DECIMAL field is constrained to a maximum of {} decimal places. Consider rounding the value or increasing the scale constraint.",
							max
						),
					),
					ConstraintKind::NoneNotAllowed { column_type } => (
						"CONSTRAINT_007",
						format!(
							"The column type is {} which does not accept none. Use Option({}) if the column should be nullable.",
							column_type, column_type
						),
					),
				};

				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					column: None,
					fragment,
					label: Some("constraint violation".to_string()),
					help: Some(help),
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::InvalidNumberFormat { target, fragment } => {
				let label = Some(format!("'{}' is not a valid {} number", fragment.text(), target));
				let (help, notes) = match target {
					Type::Float4 | Type::Float8 => (
						"use decimal format (e.g., 123.45, -67.89, 1.23e-4)".to_string(),
						vec![
							"valid: 123.45".to_string(),
							"valid: -67.89".to_string(),
							"valid: 1.23e-4".to_string(),
						],
					),
					Type::Int1
					| Type::Int2
					| Type::Int4
					| Type::Int8
					| Type::Int16
					| Type::Uint1
					| Type::Uint2
					| Type::Uint4
					| Type::Uint8
					| Type::Uint16 => (
						"use integer format (e.g., 123, -456) or decimal that can be truncated".to_string(),
						vec![
							"valid: 123".to_string(),
							"valid: -456".to_string(),
							"truncated: 123.7 → 123".to_string(),
						],
					),
					_ => (
						"ensure the value is a valid number".to_string(),
						vec!["use a proper number format".to_string()],
					),
				};

				Diagnostic {
					code: "NUMBER_001".to_string(),
					statement: None,
					message: "invalid number format".to_string(),
					fragment,
					label,
					help: Some(help),
					notes,
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::NumberOutOfRange {
				target,
				fragment,
				descriptor,
			} => {
				let range = value_range(target.clone());

				let label = if let Some(ref desc) = descriptor {
					Some(format!(
						"value '{}' exceeds the valid range for {} column {}",
						fragment.text(),
						desc.column_type.as_ref().unwrap_or(&target),
						desc.location_string()
					))
				} else {
					Some(format!(
						"value '{}' exceeds the valid range for type {} ({})",
						fragment.text(),
						target,
						range
					))
				};

				let help = if let Some(ref desc) = descriptor {
					if desc.namespace.is_some() && desc.table.is_some() {
						Some(format!("use a value within range {} or modify column {}", range, desc.location_string()))
					} else {
						Some(format!("use a value within range {} or use a wider type", range))
					}
				} else {
					Some(format!("use a value within range {} or use a wider type", range))
				};

				Diagnostic {
					code: "NUMBER_002".to_string(),
					statement: None,
					message: "number out of range".to_string(),
					fragment,
					label,
					help,
					notes: vec![format!("valid range: {}", range)],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::NanNotAllowed => Diagnostic {
				code: "NUMBER_003".to_string(),
				statement: None,
				message: "NaN not allowed".to_string(),
				fragment: Fragment::None,
				label: Some("NaN (Not a Number) values are not permitted".to_string()),
				help: Some("use a finite number or none instead".to_string()),
				notes: vec![],
				column: None,
				cause: None,
				operator_chain: None,
			},

			TypeError::IntegerPrecisionLoss {
				source_type,
				target,
				fragment,
			} => {
				let is_signed = source_type.is_signed_integer();
				let (min_limit, max_limit) = match target {
					Type::Float4 => {
						if is_signed {
							("-16_777_216 (-2^24)", "16_777_216 (2^24)")
						} else {
							("0", "16_777_216 (2^24)")
						}
					}
					Type::Float8 => {
						if is_signed {
							("-9_007_199_254_740_992 (-2^53)", "9_007_199_254_740_992 (2^53)")
						} else {
							("0", "9_007_199_254_740_992 (2^53)")
						}
					}
					_ => unreachable!("IntegerPrecisionLoss should only be used for float targets"),
				};

				let label = Some(format!(
					"converting '{}' from {} to {} would lose precision",
					fragment.text(),
					source_type,
					target
				));

				Diagnostic {
					code: "NUMBER_004".to_string(),
					statement: None,
					message: "too large for precise float conversion".to_string(),
					fragment,
					label,
					help: None,
					notes: vec![
						format!("{} can only represent from {} to {} precisely", target, min_limit, max_limit),
						"consider using a different numeric type if exact precision is required".to_string(),
					],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::DecimalScaleExceedsPrecision {
				scale,
				precision,
				fragment,
			} => {
				let label = Some(format!("scale ({}) cannot be greater than precision ({})", scale, precision));
				Diagnostic {
					code: "NUMBER_005".to_string(),
					statement: None,
					message: "decimal scale exceeds precision".to_string(),
					fragment,
					label,
					help: Some(format!("use a scale value between 0 and {} or increase precision", precision)),
					notes: vec![
						format!("current precision: {}", precision),
						format!("current scale: {}", scale),
						"scale represents the number of digits after the decimal point".to_string(),
						"precision represents the total number of significant digits".to_string(),
					],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::DecimalPrecisionInvalid { precision } => {
				let label = Some(format!("precision ({}) must be at least 1", precision));
				Diagnostic {
					code: "NUMBER_006".to_string(),
					statement: None,
					message: "invalid decimal precision".to_string(),
					fragment: Fragment::None,
					label,
					help: Some("use a precision value of at least 1".to_string()),
					notes: vec![
						format!("current precision: {}", precision),
						"precision represents the total number of significant digits".to_string(),
					],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::InvalidBooleanFormat { fragment } => {
				let value = fragment.text().to_string();
				let label = Some(format!("expected 'true' or 'false', found '{}'", value));
				Diagnostic {
					code: "BOOLEAN_001".to_string(),
					statement: None,
					message: "invalid boolean format".to_string(),
					fragment,
					label,
					help: Some("use 'true' or 'false'".to_string()),
					notes: vec!["valid: true, TRUE".to_string(), "valid: false, FALSE".to_string()],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::EmptyBooleanValue { fragment } => Diagnostic {
				code: "BOOLEAN_002".to_string(),
				statement: None,
				message: "empty boolean value".to_string(),
				fragment,
				label: Some("boolean value cannot be empty".to_string()),
				help: Some("provide either 'true' or 'false'".to_string()),
				notes: vec!["valid: true".to_string(), "valid: false".to_string()],
				column: None,
				cause: None,
				operator_chain: None,
			},

			TypeError::InvalidNumberBoolean { fragment } => {
				let value = fragment.text().to_string();
				let label =
					Some(format!("number '{}' cannot be cast to boolean, only 1 or 0 are allowed", value));
				Diagnostic {
					code: "BOOLEAN_003".to_string(),
					statement: None,
					message: "invalid boolean".to_string(),
					fragment,
					label,
					help: Some("use 1 for true or 0 for false".to_string()),
					notes: vec![
						"valid: 1 → true".to_string(),
						"valid: 0 → false".to_string(),
						"invalid: any other number".to_string(),
					],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::Temporal {
				kind,
				message,
				fragment,
			} => {
				let (code, label, help, notes) = match &kind {
					TemporalKind::InvalidDateFormat => (
						"TEMPORAL_001",
						Some(format!("expected YYYY-MM-DD format, found '{}'", fragment.text())),
						Some("use the format YYYY-MM-DD (e.g., 2024-03-15)".to_string()),
						vec!["dates must have exactly 3 parts separated by hyphens".to_string()],
					),
					TemporalKind::InvalidDateTimeFormat => (
						"TEMPORAL_002",
						Some(format!("expected YYYY-MM-DDTHH:MM:SS format, found '{}'", fragment.text())),
						Some("use the format YYYY-MM-DDTHH:MM:SS[.fff][Z|±HH:MM] (e.g., 2024-03-15T14:30:45)".to_string()),
						vec!["datetime must contain 'T' separator between date and time parts".to_string()],
					),
					TemporalKind::InvalidTimeFormat => (
						"TEMPORAL_003",
						Some(format!("expected HH:MM:SS format, found '{}'", fragment.text())),
						Some("use the format HH:MM:SS[.fff][Z|±HH:MM] (e.g., 14:30:45)".to_string()),
						vec!["time must have exactly 3 parts separated by colons".to_string()],
					),
					TemporalKind::InvalidDurationFormat => (
						"TEMPORAL_004",
						Some(format!("expected P[n]Y[n]M[n]W[n]D[T[n]H[n]M[n]S] format, found '{}'", fragment.text())),
						Some("use ISO 8601 duration format starting with 'P' (e.g., P1D, PT2H30M, P1Y2M3DT4H5M6S)".to_string()),
						vec![
							"duration must start with 'P' followed by duration components".to_string(),
							"date part: P[n]Y[n]M[n]W[n]D (years, months, weeks, days)".to_string(),
							"time part: T[n]H[n]M[n]S (hours, minutes, seconds)".to_string(),
						],
					),
					TemporalKind::InvalidYear => (
						"TEMPORAL_005",
						Some(format!("year '{}' cannot be parsed as a number", fragment.text())),
						Some("ensure the year is a valid 4-digit number".to_string()),
						vec!["valid examples: 2024, 1999, 2000".to_string()],
					),
					TemporalKind::InvalidTimeComponentFormat { component } => (
						"TEMPORAL_005",
						Some(format!("{} '{}' must be exactly 2 digits", component, fragment.text())),
						Some(format!("ensure the {} is exactly 2 digits (e.g., 09, 14, 23)", component)),
						vec![format!("{} must be exactly 2 digits in HH:MM:SS format", component)],
					),
					TemporalKind::InvalidMonth => (
						"TEMPORAL_006",
						Some(format!("month '{}' cannot be parsed as a number (expected 1-12)", fragment.text())),
						Some("ensure the month is a valid number between 1 and 12".to_string()),
						vec!["valid examples: 01, 03, 12".to_string()],
					),
					TemporalKind::InvalidDay => (
						"TEMPORAL_007",
						Some(format!("day '{}' cannot be parsed as a number (expected 1-31)", fragment.text())),
						Some("ensure the day is a valid number between 1 and 31".to_string()),
						vec!["valid examples: 01, 15, 31".to_string()],
					),
					TemporalKind::InvalidHour => (
						"TEMPORAL_008",
						Some(format!("hour '{}' cannot be parsed as a number (expected 0-23)", fragment.text())),
						Some("ensure the hour is a valid number between 0 and 23 (use 24-hour format)".to_string()),
						vec![
							"valid examples: 09, 14, 23".to_string(),
							"hours must be in 24-hour format (00-23)".to_string(),
						],
					),
					TemporalKind::InvalidMinute => (
						"TEMPORAL_009",
						Some(format!("minute '{}' cannot be parsed as a number (expected 0-59)", fragment.text())),
						Some("ensure the minute is a valid number between 0 and 59".to_string()),
						vec!["valid examples: 00, 30, 59".to_string()],
					),
					TemporalKind::InvalidSecond => (
						"TEMPORAL_010",
						Some(format!("second '{}' cannot be parsed as a number (expected 0-59)", fragment.text())),
						Some("ensure the second is a valid number between 0 and 59".to_string()),
						vec!["valid examples: 00, 30, 59".to_string()],
					),
					TemporalKind::InvalidFractionalSeconds => (
						"TEMPORAL_011",
						Some(format!("fractional seconds '{}' cannot be parsed as a number", fragment.text())),
						Some("ensure fractional seconds contain only digits".to_string()),
						vec!["valid examples: 123, 999999, 000001".to_string()],
					),
					TemporalKind::InvalidDateValues => (
						"TEMPORAL_012",
						Some(format!("date '{}' represents an invalid calendar date", fragment.text())),
						Some("ensure the date exists in the calendar (e.g., no February 30)".to_string()),
						vec![
							"check month has correct number of days".to_string(),
							"consider leap years for February 29".to_string(),
						],
					),
					TemporalKind::InvalidTimeValues => (
						"TEMPORAL_013",
						Some(format!("time '{}' contains out-of-range values", fragment.text())),
						Some("ensure hours are 0-23, minutes and seconds are 0-59".to_string()),
						vec!["use 24-hour format for hours".to_string()],
					),
					TemporalKind::InvalidDurationCharacter => (
						"TEMPORAL_014",
						Some(format!("character '{}' is not valid in ISO 8601 duration", fragment.text())),
						Some("use only valid duration units: Y, M, W, D, H, m, S".to_string()),
						vec![
							"date part units: Y (years), M (months), W (weeks), D (days)".to_string(),
							"time part units: H (hours), m (minutes), S (seconds)".to_string(),
						],
					),
					TemporalKind::IncompleteDurationSpecification => (
						"TEMPORAL_015",
						Some(format!("number '{}' is missing a unit specifier", fragment.text())),
						Some("add a unit letter after the number (Y, M, W, D, H, M, or S)".to_string()),
						vec!["example: P1D (not P1), PT2H (not PT2)".to_string()],
					),
					TemporalKind::InvalidUnitInContext { unit, in_time_part } => {
						let context = if *in_time_part {
							"time part (after T)"
						} else {
							"date part (before T)"
						};
						let allowed = if *in_time_part { "H, M, S" } else { "Y, M, W, D" };
						(
							"TEMPORAL_016",
							Some(format!("unit '{}' is not allowed in the {}", unit, context)),
							Some(format!("use only {} in the {}", allowed, context)),
							vec![
								"date part (before T): Y, M, W, D".to_string(),
								"time part (after T): H, M, S".to_string(),
							],
						)
					}
					TemporalKind::InvalidDurationComponentValue { unit } => (
						"TEMPORAL_017",
						Some(format!("{} value '{}' cannot be parsed as a number", temporal_unit_name(*unit), fragment.text())),
						Some(format!("ensure the {} value is a valid number", temporal_unit_name(*unit))),
						vec![format!("valid examples: P1{}, P10{}", unit, unit)],
					),
					TemporalKind::UnrecognizedTemporalPattern => (
						"TEMPORAL_018",
						Some(format!("value '{}' does not match any temporal format", fragment.text())),
						Some("use one of the supported formats: date (YYYY-MM-DD), time (HH:MM:SS), datetime (YYYY-MM-DDTHH:MM:SS), or duration (P...)".to_string()),
						vec![
							"date: 2024-03-15".to_string(),
							"time: 14:30:45".to_string(),
							"datetime: 2024-03-15T14:30:45".to_string(),
							"duration: P1Y2M3DT4H5M6S".to_string(),
						],
					),
					TemporalKind::EmptyDateComponent => (
						"TEMPORAL_019",
						Some(format!("date component '{}' is empty", fragment.text())),
						Some("ensure all date parts (year, month, day) are provided".to_string()),
						vec!["date format: YYYY-MM-DD (e.g., 2024-03-15)".to_string()],
					),
					TemporalKind::EmptyTimeComponent => (
						"TEMPORAL_020",
						Some(format!("time component '{}' is empty", fragment.text())),
						Some("ensure all time parts (hour, minute, second) are provided".to_string()),
						vec!["time format: HH:MM:SS (e.g., 14:30:45)".to_string()],
					),
					TemporalKind::DuplicateDurationComponent { component } => (
						"TEMPORAL_021",
						Some(format!("duration component '{}' appears multiple times", component)),
						Some("each duration component (Y, M, W, D, H, M, S) can only appear once".to_string()),
						vec!["valid: P1Y2M3D".to_string(), "invalid: P1Y2Y (duplicate Y)".to_string()],
					),
					TemporalKind::OutOfOrderDurationComponent { component } => (
						"TEMPORAL_022",
						Some(format!("duration component '{}' appears out of order", component)),
						Some("duration components must appear in order: Y, M, W, D (before T), then H, M, S (after T)".to_string()),
						vec![
							"date part order: Y (years), M (months), W (weeks), D (days)".to_string(),
							"time part order: H (hours), M (minutes), S (seconds)".to_string(),
							"valid: P1Y2M3D, PT1H2M3S".to_string(),
							"invalid: P1D1Y (D before Y), PT1S1H (S before H)".to_string(),
						],
					),
				};

				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					fragment,
					label,
					help,
					notes,
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::InvalidUuid4Format { fragment } => {
				let label = Some(format!("'{}' is not a valid UUID v4", fragment.text()));
				Diagnostic {
					code: "UUID_001".to_string(),
					statement: None,
					message: "invalid UUID v4 format".to_string(),
					fragment,
					label,
					help: Some("use UUID v4 format (e.g., 550e8400-e29b-41d4-a716-446655440000)".to_string()),
					notes: vec![
						"valid: 550e8400-e29b-41d4-a716-446655440000".to_string(),
						"valid: f47ac10b-58cc-4372-a567-0e02b2c3d479".to_string(),
						"UUID v4 uses random or pseudo-random numbers".to_string(),
					],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::InvalidUuid7Format { fragment } => {
				let label = Some(format!("'{}' is not a valid UUID v7", fragment.text()));
				Diagnostic {
					code: "UUID_002".to_string(),
					statement: None,
					message: "invalid UUID v7 format".to_string(),
					fragment,
					label,
					help: Some("use UUID v7 format (e.g., 017f22e2-79b0-7cc3-98c4-dc0c0c07398f)".to_string()),
					notes: vec![
						"valid: 017f22e2-79b0-7cc3-98c4-dc0c0c07398f".to_string(),
						"valid: 01854d6e-bd60-7b28-a3c7-6b4ad2c4e2e8".to_string(),
						"UUID v7 uses timestamp-based generation".to_string(),
					],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::BlobEncoding {
				kind,
				message,
				fragment,
			} => {
				let (code, label, help) = match &kind {
					BlobEncodingKind::InvalidHex => (
						"BLOB_001",
						Some("Invalid hex characters found".to_string()),
						Some("Hex strings should only contain 0-9, a-f, A-F characters".to_string()),
					),
					BlobEncodingKind::InvalidBase64 => (
						"BLOB_002",
						Some("Invalid base64 encoding found".to_string()),
						Some("Base64 strings should only contain A-Z, a-z, 0-9, +, / and = padding".to_string()),
					),
					BlobEncodingKind::InvalidBase64Url => (
						"BLOB_003",
						Some("Invalid base64url encoding found".to_string()),
						Some("Base64url strings should only contain A-Z, a-z, 0-9, -, _ characters".to_string()),
					),
					BlobEncodingKind::InvalidBase58 => (
						"BLOB_005",
						Some("Invalid base58 encoding found".to_string()),
						Some("Base58 strings should only contain 1-9, A-H, J-N, P-Z, a-k, m-z characters".to_string()),
					),
					BlobEncodingKind::InvalidUtf8Sequence { .. } => (
						"BLOB_004",
						Some("BLOB contains invalid UTF-8 bytes".to_string()),
						Some("Use to_utf8_lossy() if you want to replace invalid sequences with replacement characters".to_string()),
					),
				};

				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					column: None,
					fragment,
					label,
					help,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::SerdeDeserialize { message } => Diagnostic {
				code: "SERDE_001".to_string(),
				statement: None,
				message: format!("Serde deserialization error: {}", message),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Check data format and structure".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TypeError::SerdeSerialize { message } => Diagnostic {
				code: "SERDE_002".to_string(),
				statement: None,
				message: format!("Serde serialization error: {}", message),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Check data format and structure".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TypeError::SerdeKeycode { message } => Diagnostic {
				code: "SERDE_003".to_string(),
				statement: None,
				message: format!("Keycode serialization error: {}", message),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Check keycode data and format".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TypeError::ArrayConversion { message } => Diagnostic {
				code: "CONV_001".to_string(),
				statement: None,
				message: format!("Array conversion error: {}", message),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Check array size requirements".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TypeError::Utf8Conversion { message } => Diagnostic {
				code: "CONV_002".to_string(),
				statement: None,
				message: format!("UTF-8 conversion error: {}", message),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Check string encoding".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TypeError::IntegerConversion { message } => Diagnostic {
				code: "CONV_003".to_string(),
				statement: None,
				message: format!("Integer conversion error: {}", message),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: Some("Check integer range limits".to_string()),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TypeError::Network { kind, message } => {
				let (code, help) = match &kind {
					NetworkErrorKind::Connection { .. } => {
						("NET_001", Some("Check network connectivity and server status".to_string()))
					}
					NetworkErrorKind::Engine { .. } => ("NET_002", None),
					NetworkErrorKind::Transport { .. } => {
						("NET_003", Some("Check network connectivity".to_string()))
					}
					NetworkErrorKind::Status { .. } => {
						("NET_004", Some("Check gRPC service status".to_string()))
					}
				};

				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					column: None,
					fragment: Fragment::None,
					label: None,
					help,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::Auth { kind, message } => {
				let (code, help) = match &kind {
					AuthErrorKind::AuthenticationFailed { .. } => {
						("ASVTH_001", Some("Check your credentials and try again".to_string()))
					}
					AuthErrorKind::AuthorizationDenied { .. } => {
						("ASVTH_002", Some("Check your permissions for this resource".to_string()))
					}
					AuthErrorKind::TokenExpired => {
						("ASVTH_003", Some("Refresh your authentication token".to_string()))
					}
					AuthErrorKind::InvalidToken => {
						("ASVTH_004", Some("Provide a valid authentication token".to_string()))
					}
				};

				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					column: None,
					fragment: Fragment::None,
					label: None,
					help,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::DictionaryCapacityExceeded {
				id_type,
				value,
				max_value,
			} => Diagnostic {
				code: "DICT_001".to_string(),
				statement: None,
				message: format!("dictionary entry ID {} exceeds maximum {} for type {}", value, max_value, id_type),
				column: None,
				fragment: Fragment::None,
				label: Some(format!("{} capacity exceeded", id_type)),
				help: Some(
					"use a larger ID type (e.g., Uint2 instead of Uint1) when creating the dictionary".to_string(),
				),
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			TypeError::AssertionFailed {
				fragment,
				message,
				expression,
			} => {
				let label = expression
					.as_ref()
					.map(|expr| format!("this expression is false: {}", expr))
					.or_else(|| Some("assertion failed".to_string()));

				Diagnostic {
					code: "ASSERT".to_string(),
					statement: None,
					message,
					fragment,
					label,
					help: None,
					notes: vec![],
					column: None,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::Function {
				kind,
				message,
				fragment,
			} => {
				let (code, label, help) = match &kind {
					FunctionErrorKind::UnknownFunction => (
						"FUNCTION_001",
						Some("unknown function".to_string()),
						Some("Check the function name and available functions".to_string()),
					),
					FunctionErrorKind::ArityMismatch { expected, .. } => (
						"FUNCTION_002",
						Some("wrong number of arguments".to_string()),
						Some(format!("Provide exactly {} arguments to function {}", expected, fragment.text())),
					),
					FunctionErrorKind::TooManyArguments { max_args, .. } => (
						"FUNCTION_003",
						Some("too many arguments".to_string()),
						Some(format!("Provide at most {} arguments to function {}", max_args, fragment.text())),
					),
					FunctionErrorKind::InvalidArgumentType { expected, .. } => {
						let expected_types =
							expected.iter().map(|t| format!("{:?}", t)).collect::<Vec<_>>().join(", ");
						(
							"FUNCTION_004",
							Some("invalid argument type".to_string()),
							Some(format!("Provide an argument of type: {}", expected_types)),
						)
					}
					FunctionErrorKind::UndefinedArgument { .. } => (
						"FUNCTION_005",
						Some("none argument".to_string()),
						Some("Provide a defined value for this argument".to_string()),
					),
					FunctionErrorKind::MissingInput => (
						"FUNCTION_006",
						Some("missing input".to_string()),
						Some("Provide input data to the function".to_string()),
					),
					FunctionErrorKind::ExecutionFailed { .. } => (
						"FUNCTION_007",
						Some("execution failed".to_string()),
						Some("Check function arguments and data".to_string()),
					),
					FunctionErrorKind::InternalError { .. } => (
						"FUNCTION_008",
						Some("internal error".to_string()),
						Some("This is an internal error - please report this issue".to_string()),
					),
					FunctionErrorKind::GeneratorNotFound => (
						"FUNCTION_009",
						Some("unknown generator function".to_string()),
						Some("Check the generator function name and ensure it is registered".to_string()),
					),
				};

				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					column: None,
					fragment,
					label,
					help,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::Ast {
				kind,
				message,
				fragment,
			} => {
				let (code, label, help) = match &kind {
					AstErrorKind::TokenizeError { .. } => (
						"AST_001",
						None,
						Some("Check syntax and token format".to_string()),
					),
					AstErrorKind::UnexpectedEof => (
						"AST_002",
						None,
						Some("Complete the statement".to_string()),
					),
					AstErrorKind::ExpectedIdentifier => (
						"AST_003",
						Some(format!("found `{}`", fragment.text())),
						Some("expected token of type `identifier`".to_string()),
					),
					AstErrorKind::InvalidColumnProperty => (
						"AST_011",
						Some(format!("found `{}`", fragment.text())),
						Some("Expected one of: auto_increment, dictionary, saturation, default".to_string()),
					),
					AstErrorKind::InvalidPolicy => (
						"AST_004",
						Some(format!("found `{}`", fragment.text())),
						Some("Expected a valid policy identifier".to_string()),
					),
					AstErrorKind::UnexpectedToken { expected } => (
						"AST_005",
						Some(format!("found `{}`", fragment.text())),
						Some(format!("Use {} instead", expected)),
					),
					AstErrorKind::UnsupportedToken => (
						"AST_006",
						Some(format!("found `{}`", fragment.text())),
						Some("This token is not supported in this context".to_string()),
					),
					AstErrorKind::MultipleExpressionsWithoutBraces => {
						let keyword = fragment.text().to_string();
						(
							"AST_007",
							Some("missing `{ … }` around expressions".to_string()),
							Some(format!("wrap the expressions in curly braces:\n    {} {{ expr1, expr2, … }}", keyword)),
						)
					}
					AstErrorKind::UnrecognizedType => (
						"AST_008",
						Some("type not found".to_string()),
						None,
					),
					AstErrorKind::UnsupportedAstNode { .. } => (
						"AST_009",
						Some("not supported in this context".to_string()),
						Some("This syntax is not yet supported or may be invalid in this context".to_string()),
					),
					AstErrorKind::EmptyPipeline => (
						"AST_010",
						None,
						Some("A query pipeline must contain at least one operation".to_string()),
					),
				};

				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					column: None,
					fragment,
					label,
					help,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::Runtime { kind, message } => {
				let (code, help) = match &kind {
					RuntimeErrorKind::VariableNotFound { name } => (
						"RUNTIME_001",
						Some(format!("Define the variable using 'let {} = <value>' before using it", name)),
					),
					RuntimeErrorKind::VariableIsDataframe { name } => (
						"RUNTIME_002",
						Some(format!(
							"Extract a scalar value from the dataframe using '${} | only()', '${} | first()', or '${} | first_or_none()'",
							name, name, name
						)),
					),
					RuntimeErrorKind::VariableIsImmutable { .. } => (
						"RUNTIME_003",
						Some("Use 'let mut $name := value' to declare a mutable variable".to_string()),
					),
					RuntimeErrorKind::BreakOutsideLoop => (
						"RUNTIME_004",
						Some("Use BREAK inside a LOOP, WHILE, or FOR block".to_string()),
					),
					RuntimeErrorKind::ContinueOutsideLoop => (
						"RUNTIME_005",
						Some("Use CONTINUE inside a LOOP, WHILE, or FOR block".to_string()),
					),
					RuntimeErrorKind::MaxIterationsExceeded { .. } => (
						"RUNTIME_006",
						Some("Add a BREAK condition or use WHILE with a terminating condition".to_string()),
					),
					RuntimeErrorKind::UndefinedFunction { .. } => (
						"RUNTIME_007",
						Some("Define the function using 'DEF name [] { ... }' before calling it".to_string()),
					),
					RuntimeErrorKind::FieldNotFound {
						variable, available, ..
					} => {
						let help = if available.is_empty() {
							format!("The variable '{}' has no fields", variable)
						} else {
							format!("Available fields: {}", available.join(", "))
						};
						("RUNTIME_009", Some(help))
					}
					RuntimeErrorKind::AppendTargetNotFrame { .. } => (
						"RUNTIME_008",
						Some("APPEND can only target Frame variables. Use a new variable name or ensure the target was created by APPEND or FROM".to_string()),
					),
				};

				let notes = match &kind {
					RuntimeErrorKind::VariableIsImmutable { .. } => {
						vec!["Only mutable variables can be reassigned".to_string()]
					}
					RuntimeErrorKind::VariableIsDataframe { .. } => {
						vec![
							"Dataframes must be explicitly converted to scalar values before use in expressions".to_string(),
							"Use only() for exactly 1 row × 1 column dataframes".to_string(),
							"Use first() to take the first value from the first column".to_string(),
						]
					}
					_ => vec![],
				};

				let fragment = match &kind {
					RuntimeErrorKind::UndefinedFunction { name } => Fragment::internal(name.clone()),
					_ => Fragment::None,
				};

				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					column: None,
					fragment,
					label: None,
					help,
					notes,
					cause: None,
					operator_chain: None,
				}
			}

			TypeError::Procedure { kind, message, fragment } => {
				let (code, help, label) = match &kind {
					ProcedureErrorKind::UndefinedProcedure { .. } => (
						"PROCEDURE_001",
						"Check the procedure name and available procedures",
						"unknown procedure",
					),
				};

				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					column: None,
					fragment,
					label: Some(label.to_string()),
					help: Some(help.to_string()),
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}
		}
	}
}
