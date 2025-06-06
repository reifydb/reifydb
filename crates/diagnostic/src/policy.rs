// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Diagnostic, DiagnosticColumn, Span};
use reifydb_core::ValueKind;

pub struct ColumnOverflow {
    pub span: Span,
    pub column: String,
    pub value: ValueKind,
}

pub fn column_overflow<'a>(co: ColumnOverflow) -> Diagnostic {
    let label = Some(format!(
        "value `{}` does not fit into `{}` (range: {})",
        &co.span.fragment,
        co.value,
        column_range(co.value)
    ));

    Diagnostic {
        code: "PO0001".to_string(),
        message: format!("value overflows column `{}` type `{}`", co.column, co.value),
        span: Some(co.span),
        label,
        help: Some("reduce the value, change the column type to a wider type or change the overflow policy".to_string()),
        notes: vec![],
        column: Some(DiagnosticColumn { name: co.column, value: co.value }),
    }
}

pub struct ColumnUnderflow {
    pub span: Span,
    pub column_name: String,
    pub column_value: ValueKind,
}

pub fn column_underflow<'a>(co: ColumnUnderflow) -> Diagnostic {
    let label = Some(format!(
        "value `{}` does not fit into `{}` (range: {})",
        &co.span.fragment,
        co.column_value,
        column_range(co.column_value)
    ));

    Diagnostic {
        code: "PO0002".to_string(),
        message: format!("value underflows column `{}` type `{}`", co.column_name, co.column_value),
        span: Some(co.span),
        label,
        help: Some("increase the value, change the column type to a wider type or change the underflow policy".to_string()),
        notes: vec![],
        column: Some(DiagnosticColumn { name: co.column_name, value: co.column_value }),
    }
}

fn column_range<'a>(value: ValueKind) -> &'a str {
    match value {
        ValueKind::Bool => unreachable!(),
        ValueKind::Float4 => "-3.4e38 to +3.4e38",
        ValueKind::Float8 => "-1.8e308 to +1.8e308",
        ValueKind::Int1 => "-128 to 127",
        ValueKind::Int2 => "-32,768 to 32,767",
        ValueKind::Int4 => "-2,147,483,648 to 2,147,483,647",
        ValueKind::Int8 => "-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807",
        ValueKind::Int16 => {
            "-170141183460469231731687303715884105728 to 170141183460469231731687303715884105727"
        }
        ValueKind::String => unreachable!(),
        ValueKind::Uint1 => "0 to 255",
        ValueKind::Uint2 => "0 to 65,535",
        ValueKind::Uint4 => "0 to 4,294,967,295",
        ValueKind::Uint8 => "0 to 18,446,744,073,709,551,615",
        ValueKind::Uint16 => "0 to 340,282,366,920,938,463,463,374,607,431,768,211,455",
        ValueKind::Undefined => unreachable!(),
    }
}

#[cfg(test)]
mod tests {

    mod column_range {
        use crate::policy::column_range;
        use reifydb_core::ValueKind;

        #[test]
        fn test_signed_ints() {
            assert_eq!(column_range(ValueKind::Int1), "-128 to 127");
            assert_eq!(column_range(ValueKind::Int2), "-32,768 to 32,767");
            assert_eq!(column_range(ValueKind::Int4), "-2,147,483,648 to 2,147,483,647");
            assert_eq!(
                column_range(ValueKind::Int8),
                "-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807"
            );
            assert_eq!(
                column_range(ValueKind::Int16),
                "-170141183460469231731687303715884105728 to 170141183460469231731687303715884105727"
            );
        }

        #[test]
        fn test_unsigned_ints() {
            assert_eq!(column_range(ValueKind::Uint1), "0 to 255");
            assert_eq!(column_range(ValueKind::Uint2), "0 to 65,535");
            assert_eq!(column_range(ValueKind::Uint4), "0 to 4,294,967,295");
            assert_eq!(column_range(ValueKind::Uint8), "0 to 18,446,744,073,709,551,615");
            assert_eq!(
                column_range(ValueKind::Uint16),
                "0 to 340,282,366,920,938,463,463,374,607,431,768,211,455"
            );
        }

        #[test]
        fn test_floats() {
            assert_eq!(column_range(ValueKind::Float4), "-3.4e38 to +3.4e38");
            assert_eq!(column_range(ValueKind::Float8), "-1.8e308 to +1.8e308");
        }
    }
}
