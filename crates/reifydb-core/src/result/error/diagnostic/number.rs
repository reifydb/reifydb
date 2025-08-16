// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ColumnDescriptor, IntoDiagnosticOrigin, DiagnosticOrigin, Type,
	result::error::diagnostic::{Diagnostic, util::value_range},
};

pub fn invalid_number_format(
	origin: impl IntoDiagnosticOrigin,
	target: Type,
) -> Diagnostic {
	let origin = origin.into_origin();
	let label = Some(format!(
		"'{}' is not a valid {} number",
		origin.fragment().unwrap_or(""), target
	));

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
		origin: origin,
		label,
		help: Some(help),
		notes,
		column: None,
		cause: None,
	}
}

pub fn number_out_of_range(
	origin: impl IntoDiagnosticOrigin,
	target: Type,
	descriptor: Option<&ColumnDescriptor>,
) -> Diagnostic {
	let origin = origin.into_origin();

	let range = value_range(target);

	let label = if let Some(desc) = descriptor {
		Some(format!(
			"value '{}' exceeds the valid range for {} column {}",
			origin.fragment().unwrap_or(""),
			desc.column_type.as_ref().unwrap_or(&target),
			desc.location_string()
		))
	} else {
		Some(format!(
			"value '{}' exceeds the valid range for type {} ({})",
			origin.fragment().unwrap_or(""), target, range
		))
	};

	let help = if let Some(desc) = descriptor {
		if desc.schema.is_some() && desc.table.is_some() {
			Some(format!(
				"use a value within range {} or modify column {}",
				range,
				desc.location_string()
			))
		} else {
			Some(format!(
				"use a value within range {} or use a wider type",
				range
			))
		}
	} else {
		Some(format!(
			"use a value within range {} or use a wider type",
			range
		))
	};

	let notes = vec![format!("valid range: {}", range)];
	Diagnostic {
		code: "NUMBER_002".to_string(),
		statement: None,
		message: "number out of range".to_string(),
		origin: origin,
		label,
		help,
		notes,
		column: None,
		cause: None,
	}
}

pub fn nan_not_allowed() -> Diagnostic {
	let label =
		Some("NaN (Not a Number) values are not permitted".to_string());

	Diagnostic {
		code: "NUMBER_003".to_string(),
		statement: None,
		message: "NaN not allowed".to_string(),
		origin: DiagnosticOrigin::None,
		label,
		help: Some(
			"use a finite number or undefined instead".to_string()
		),
		notes: vec![],
		column: None,
		cause: None,
	}
}

pub fn integer_precision_loss(
	origin: impl IntoDiagnosticOrigin,
	source_type: Type,
	target: Type,
) -> Diagnostic {
	let origin = origin.into_origin();
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
				(
					"-9_007_199_254_740_992 (-2^53)",
					"9_007_199_254_740_992 (2^53)",
				)
			} else {
				("0", "9_007_199_254_740_992 (2^53)")
			}
		}
		_ => {
			unreachable!(
				"precision_loss_on_float_conversion should only be called for float types"
			)
		}
	};

	let label = Some(format!(
		"converting '{}' from {} to {} would lose precision",
		origin.fragment().unwrap_or(""), source_type, target
	));

	Diagnostic {
        code: "NUMBER_004".to_string(),
        statement: None,
        message: "too large for precise float conversion".to_string(),
        origin: origin,
        label,
        help: None,
        notes: vec![
            format!("{} can only represent from {} to {} precisely", target, min_limit, max_limit),
            "consider using a different numeric type if exact precision is required".to_string(),
        ],
        column: None,
        cause: None,
    }
}
