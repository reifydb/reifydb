// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use crate::{
	error::diagnostic::{Diagnostic, util::value_range},
	fragment::Fragment,
	value::{
		constraint::{precision::Precision, scale::Scale},
		r#type::Type,
	},
};

pub fn invalid_number_format(fragment: Fragment, target: Type) -> Diagnostic {
	let label = Some(format!("'{}' is not a valid {} number", fragment.text(), target));

	let (help, notes) = match target {
		Type::Float4 | Type::Float8 => (
			"use decimal format (e.g., 123.45, -67.89, 1.23e-4)".to_string(),
			vec!["valid: 123.45".to_string(), "valid: -67.89".to_string(), "valid: 1.23e-4".to_string()],
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
			vec!["valid: 123".to_string(), "valid: -456".to_string(), "truncated: 123.7 → 123".to_string()],
		),
		_ => ("ensure the value is a valid number".to_string(), vec!["use a proper number format".to_string()]),
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

// TODO remove this
pub struct NumberOfRangeColumnDescriptor<'a> {
	pub namespace: Option<&'a str>,
	pub table: Option<&'a str>,
	pub column: Option<&'a str>,
	pub column_type: Option<Type>,
}

impl<'a> Default for NumberOfRangeColumnDescriptor<'a> {
	fn default() -> Self {
		Self {
			namespace: None,
			table: None,
			column: None,
			column_type: None,
		}
	}
}

impl<'a> NumberOfRangeColumnDescriptor<'a> {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_namespace(mut self, namespace: &'a str) -> Self {
		self.namespace = Some(namespace);
		self
	}

	pub fn with_table(mut self, table: &'a str) -> Self {
		self.table = Some(table);
		self
	}

	pub fn with_column(mut self, column: &'a str) -> Self {
		self.column = Some(column);
		self
	}

	pub fn with_column_type(mut self, column_type: Type) -> Self {
		self.column_type = Some(column_type);
		self
	}

	// Location formatting
	pub fn location_string(&self) -> String {
		match (self.namespace, self.table, self.column) {
			(Some(s), Some(t), Some(c)) => {
				format!("{}.{}.{}", s, t, c)
			}
			(Some(s), Some(t), None) => format!("{}.{}", s, t),
			(None, Some(t), Some(c)) => format!("{}.{}", t, c),
			(Some(s), None, Some(c)) => format!("{}.{}", s, c),
			(Some(s), None, None) => s.to_string(),
			(None, Some(t), None) => t.to_string(),
			(None, None, Some(c)) => c.to_string(),
			(None, None, None) => "unknown location".to_string(),
		}
	}
}

pub fn number_out_of_range(
	fragment: Fragment,
	target: Type,
	descriptor: Option<&NumberOfRangeColumnDescriptor>,
) -> Diagnostic {
	let range = value_range(target);

	let label = if let Some(desc) = descriptor {
		Some(format!(
			"value '{}' exceeds the valid range for {} column {}",
			fragment.text(),
			desc.column_type.as_ref().unwrap_or(&target),
			desc.location_string()
		))
	} else {
		Some(format!("value '{}' exceeds the valid range for type {} ({})", fragment.text(), target, range))
	};

	let help = if let Some(desc) = descriptor {
		if desc.namespace.is_some() && desc.table.is_some() {
			Some(format!("use a value within range {} or modify column {}", range, desc.location_string()))
		} else {
			Some(format!("use a value within range {} or use a wider type", range))
		}
	} else {
		Some(format!("use a value within range {} or use a wider type", range))
	};

	let notes = vec![format!("valid range: {}", range)];
	Diagnostic {
		code: "NUMBER_002".to_string(),
		statement: None,
		message: "number out of range".to_string(),
		fragment,
		label,
		help,
		notes,
		column: None,
		cause: None,
		operator_chain: None,
	}
}

pub fn nan_not_allowed() -> Diagnostic {
	let label = Some("NaN (Not a Number) values are not permitted".to_string());

	Diagnostic {
		code: "NUMBER_003".to_string(),
		statement: None,
		message: "NaN not allowed".to_string(),
		fragment: Fragment::None,
		label,
		help: Some("use a finite number or undefined instead".to_string()),
		notes: vec![],
		column: None,
		cause: None,
		operator_chain: None,
	}
}

pub fn integer_precision_loss(fragment: Fragment, source_type: Type, target: Type) -> Diagnostic {
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
		_ => {
			unreachable!("precision_loss_on_float_conversion should only be called for float types")
		}
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

pub fn decimal_scale_exceeds_precision(
	fragment: Fragment,
	scale: impl Into<Scale>,
	precision: impl Into<Precision>,
) -> Diagnostic {
	let scale = scale.into();
	let precision = precision.into();
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

pub fn decimal_precision_invalid(precision: u8) -> Diagnostic {
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
