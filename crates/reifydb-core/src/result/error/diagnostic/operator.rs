// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	Type, interface::fragment::IntoFragment,
	result::error::diagnostic::Diagnostic,
};

pub fn not_can_not_applied_to_number(
	fragment: impl IntoFragment,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_001".to_string(),
        statement: None,
        message: "Cannot apply NOT operator to number".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on numeric type".to_string()),
        help: Some("The NOT operator can only be applied to boolean values. Consider using comparison operators or casting to boolean first".to_string()),
        notes: vec![
            "NOT is a logical operator that inverts boolean values (true becomes false, false becomes true)".to_string(),
            "For numeric negation, use the minus (-) operator instead".to_string(),
            "To convert numbers to boolean, use comparison operators like: value != 0".to_string()
        ],
        cause: None,
    }
}

pub fn not_can_not_applied_to_text(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_002".to_string(),
        statement: None,
        message: "Cannot apply NOT operator to text".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on text type".to_string()),
        help: Some("The NOT operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "NOT is a logical operator that inverts boolean values (true becomes false, false becomes true)".to_string(),
            "To convert text to boolean, use comparison operators like: text != '...'".to_string(),
            "For string operations, use appropriate string functions instead".to_string()
        ],
        cause: None,
    }
}

pub fn not_can_not_applied_to_temporal(
	fragment: impl IntoFragment,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_003".to_string(),
        statement: None,
        message: "Cannot apply NOT operator to temporal value".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on temporal type".to_string()),
        help: Some("The NOT operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "NOT is a logical operator that inverts boolean values (true becomes false, false becomes true)".to_string(),
            "To convert temporal values to boolean, use comparison operators like: date > '2023-01-01'".to_string(),
            "Temporal types include Date, DateTime, Time, and Interval".to_string()
        ],
        cause: None,
    }
}

pub fn not_can_not_applied_to_uuid(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_004".to_string(),
        statement: None,
        message: "Cannot apply NOT operator to UUID".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on UUID type".to_string()),
        help: Some("The NOT operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "NOT is a logical operator that inverts boolean values (true becomes false, false becomes true)".to_string(),
            "To convert UUIDs to boolean, use comparison operators like: uuid == '...'".to_string(),
            "UUID types include Uuid4 and Uuid7".to_string()
        ],
        cause: None,
    }
}

pub fn and_can_not_applied_to_number(
	fragment: impl IntoFragment,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_005".to_string(),
        statement: None,
        message: "Cannot apply AND operator to number".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on numeric type".to_string()),
        help: Some("The AND operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "AND is a logical operator that combines boolean values".to_string(),
            "To convert numbers to boolean, use comparison operators like: value != 0".to_string(),
            "For bitwise operations on integers, use the bitwise AND (&) operator instead".to_string()
        ],
        cause: None,
    }
}

pub fn and_can_not_applied_to_text(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_006".to_string(),
        statement: None,
        message: "Cannot apply AND operator to text".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on text type".to_string()),
        help: Some("The AND operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "AND is a logical operator that combines boolean values".to_string(),
            "To convert text to boolean, use comparison operators like: text != ''".to_string(),
            "For text concatenation, use the string concatenation operator (||) instead".to_string()
        ],
        cause: None,
    }
}

pub fn and_can_not_applied_to_temporal(
	fragment: impl IntoFragment,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_007".to_string(),
        statement: None,
        message: "Cannot apply AND operator to temporal value".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on temporal type".to_string()),
        help: Some("The AND operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "AND is a logical operator that combines boolean values".to_string(),
            "To convert temporal values to boolean, use comparison operators like: date > '2023-01-01'".to_string(),
            "Temporal types include Date, DateTime, Time, and Interval".to_string()
        ],
        cause: None,
    }
}

pub fn and_can_not_applied_to_uuid(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_008".to_string(),
        statement: None,
        message: "Cannot apply AND operator to UUID".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on UUID type".to_string()),
        help: Some("The AND operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "AND is a logical operator that combines boolean values".to_string(),
            "To convert UUIDs to boolean, use comparison operators like: uuid == '...'".to_string(),
            "UUID types include Uuid4 and Uuid7".to_string()
        ],
        cause: None,
    }
}

pub fn or_can_not_applied_to_number(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_009".to_string(),
        statement: None,
        message: "Cannot apply OR operator to number".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on numeric type".to_string()),
        help: Some("The OR operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "OR is a logical operator that combines boolean values".to_string(),
            "To convert numbers to boolean, use comparison operators like: value != 0".to_string(),
            "For bitwise operations on integers, use the bitwise OR (|) operator instead".to_string()
        ],
        cause: None,
    }
}

pub fn or_can_not_applied_to_text(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_010".to_string(),
        statement: None,
        message: "Cannot apply OR operator to text".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on text type".to_string()),
        help: Some("The OR operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "OR is a logical operator that combines boolean values".to_string(),
            "To convert text to boolean, use comparison operators like: text != '...'".to_string(),
            "For text concatenation, use the string concatenation operator (+) instead".to_string()
        ],
        cause: None,
    }
}

pub fn or_can_not_applied_to_temporal(
	fragment: impl IntoFragment,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_011".to_string(),
        statement: None,
        message: "Cannot apply OR operator to temporal value".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on temporal type".to_string()),
        help: Some("The OR operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "OR is a logical operator that combines boolean values".to_string(),
            "To convert temporal values to boolean, use comparison operators like: date > '2023-01-01'".to_string(),
            "Temporal types include Date, DateTime, Time, and Interval".to_string()
        ],
        cause: None,
    }
}

pub fn or_can_not_applied_to_uuid(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_012".to_string(),
        statement: None,
        message: "Cannot apply OR operator to UUID".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on UUID type".to_string()),
        help: Some("The OR operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "OR is a logical operator that combines boolean values".to_string(),
            "To convert UUIDs to boolean, use comparison operators like: uuid == '...'".to_string(),
            "UUID types include Uuid4 and Uuid7".to_string()
        ],
        cause: None,
    }
}

pub fn xor_can_not_applied_to_number(
	fragment: impl IntoFragment,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_013".to_string(),
        statement: None,
        message: "Cannot apply XOR operator to number".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on numeric type".to_string()),
        help: Some("The XOR operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "XOR is a logical operator that performs exclusive or on boolean values".to_string(),
            "To convert numbers to boolean, use comparison operators like: value != 0".to_string(),
            "For bitwise operations on integers, use the bitwise XOR (^) operator instead".to_string()
        ],
        cause: None,
    }
}

pub fn xor_can_not_applied_to_text(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_014".to_string(),
        statement: None,
        message: "Cannot apply XOR operator to text".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on text type".to_string()),
        help: Some("The XOR operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "XOR is a logical operator that performs exclusive or on boolean values".to_string(),
            "To convert text to boolean, use comparison operators like: text != '...'".to_string(),
            "XOR returns true when exactly one operand is true".to_string()
        ],
        cause: None,
    }
}

pub fn xor_can_not_applied_to_temporal(
	fragment: impl IntoFragment,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_015".to_string(),
        statement: None,
        message: "Cannot apply XOR operator to temporal value".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on temporal type".to_string()),
        help: Some("The XOR operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "XOR is a logical operator that performs exclusive or on boolean values".to_string(),
            "To convert temporal values to boolean, use comparison operators like: date > '2023-01-01'".to_string(),
            "Temporal types include Date, DateTime, Time, and Interval".to_string()
        ],
        cause: None,
    }
}

pub fn xor_can_not_applied_to_uuid(fragment: impl IntoFragment) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_016".to_string(),
        statement: None,
        message: "Cannot apply XOR operator to UUID".to_string(),
        column: None,
        fragment,
        label: Some("logical operator on UUID type".to_string()),
        help: Some("The XOR operator can only be applied to boolean values. Consider using comparison operators first".to_string()),
        notes: vec![
            "XOR is a logical operator that performs exclusive or on boolean values".to_string(),
            "To convert UUIDs to boolean, use comparison operators like: uuid == '...' ".to_string(),
            "UUID types include Uuid4 and Uuid7".to_string()
        ],
        cause: None,
    }
}

pub fn add_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	left: Type,
	right: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_017".to_string(),
        statement: None,
        message: format!("Cannot apply '+' operator to {} and {}", left, right),
        column: None,
        fragment,
        label: Some("'+' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Left operand is of type: {}", left),
            format!("Right operand is of type: {}", right),
            "Consider converting operands to compatible numeric types first".to_string(),
        ],
        cause: None,
    }
}

pub fn sub_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	left: Type,
	right: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_018".to_string(),
        statement: None,
        message: format!("Cannot apply '-' operator to {} and {}", left, right),
        column: None,
        fragment,
        label: Some("'-' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Left operand is of type: {}", left),
            format!("Right operand is of type: {}", right),
            "Consider converting operands to compatible numeric types first".to_string(),
        ],
        cause: None,
    }
}

pub fn mul_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	left: Type,
	right: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_019".to_string(),
        statement: None,
        message: format!("Cannot apply '*' operator to {} and {}", left, right),
        column: None,
        fragment,
        label: Some("'*' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Left operand is of type: {}", left),
            format!("Right operand is of type: {}", right),
            "Consider converting operands to compatible numeric types first".to_string(),
        ],
        cause: None,
    }
}

pub fn div_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	left: Type,
	right: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_020".to_string(),
        statement: None,
        message: format!("Cannot apply '/' operator to {} and {}", left, right),
        column: None,
        fragment,
        label: Some("'/' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Left operand is of type: {}", left),
            format!("Right operand is of type: {}", right),
            "Consider converting operands to compatible numeric types first".to_string(),
        ],
        cause: None,
    }
}

pub fn rem_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	left: Type,
	right: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_021".to_string(),
        statement: None,
        message: format!("Cannot apply '%' operator to {} and {}", left, right),
        column: None,
        fragment,
        label: Some("'%' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Left operand is of type: {}", left),
            format!("Right operand is of type: {}", right),
            "Consider converting operands to compatible numeric types first".to_string(),
        ],
        cause: None,
    }
}

pub fn equal_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	left: Type,
	right: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_022".to_string(),
        statement: None,
        message: format!("Cannot apply '==' operator to {} and {}", left, right),
        column: None,
        fragment,
        label: Some("'==' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Left operand is of type: {}", left),
            format!("Right operand is of type: {}", right),
            "Equality comparison is only supported between compatible types".to_string(),
        ],
        cause: None,
    }
}

pub fn not_equal_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	left: Type,
	right: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_023".to_string(),
        statement: None,
        message: format!("Cannot apply '!=' operator to {} and {}", left, right),
        column: None,
        fragment,
        label: Some("'!=' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Left operand is of type: {}", left),
            format!("Right operand is of type: {}", right),
            "Inequality comparison is only supported between compatible types".to_string(),
        ],
        cause: None,
    }
}

pub fn less_than_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	left: Type,
	right: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_024".to_string(),
        statement: None,
        message: format!("Cannot apply '<' operator to {} and {}", left, right),
        column: None,
        fragment,
        label: Some("'<' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Left operand is of type: {}", left),
            format!("Right operand is of type: {}", right),
            "Less than comparison is only supported between compatible types".to_string(),
        ],
        cause: None,
    }
}

pub fn less_than_equal_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	left: Type,
	right: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_025".to_string(),
        statement: None,
        message: format!("Cannot apply '<=' operator to {} and {}", left, right),
        column: None,
        fragment,
        label: Some("'<=' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Left operand is of type: {}", left),
            format!("Right operand is of type: {}", right),
            "Less than or equal comparison is only supported between compatible types".to_string(),
        ],
        cause: None,
    }
}

pub fn greater_than_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	left: Type,
	right: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_026".to_string(),
        statement: None,
        message: format!("Cannot apply '>' operator to {} and {}", left, right),
        column: None,
        fragment,
        label: Some("'>' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Left operand is of type: {}", left),
            format!("Right operand is of type: {}", right),
            "Greater than comparison is only supported between compatible types".to_string(),
        ],
        cause: None,
    }
}

pub fn greater_than_equal_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	left: Type,
	right: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_027".to_string(),
        statement: None,
        message: format!("Cannot apply '>=' operator to {} and {}", left, right),
        column: None,
        fragment,
        label: Some("'>=' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Left operand is of type: {}", left),
            format!("Right operand is of type: {}", right),
            "Greater than or equal comparison is only supported between compatible types"
                .to_string(),
        ],
        cause: None,
    }
}

pub fn between_cannot_be_applied_to_incompatible_types(
	fragment: impl IntoFragment,
	value_type: Type,
	range_type: Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment();
	Diagnostic {
        code: "OPERATOR_028".to_string(),
        statement: None,
        message: format!(
            "Cannot apply 'BETWEEN' operator to {} with range of {}",
            value_type, range_type
        ),
        column: None,
        fragment,
        label: Some("'BETWEEN' operator on incompatible types".to_string()),
        help: None,
        notes: vec![
            format!("Value is of type: {}", value_type),
            format!("Range bounds are of type: {}", range_type),
            "BETWEEN comparison is only supported between compatible types".to_string(),
        ],
        cause: None,
    }
}
