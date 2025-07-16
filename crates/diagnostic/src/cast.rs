// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{Diagnostic, Span};
use reifydb_core::DataType;

pub fn unsupported_cast(span: Span, from_type: DataType, to_type: DataType) -> Diagnostic {
    let label = Some(format!("cannot cast {} of type {} to {}", span.fragment, from_type, to_type));
    Diagnostic {
        code: "CAST_001".to_string(),
        statement: None,
        message: format!("unsupported cast from {} to {}", from_type, to_type),
        span: Some(span),
        label,
        help: Some("ensure the source and target types are compatible for casting".to_string()),
        notes: vec![
            "supported casts include: numeric to numeric, string to temporal, boolean to numeric"
                .to_string(),
        ],
        column: None,
        caused_by: None,
    }
}

pub fn numeric_saturation(span: Span, target_type: DataType) -> Diagnostic {
    let label = Some(format!(
        "value '{}' exceeds the valid range for type {} ({})",
        span.fragment,
        target_type,
        get_type_range(target_type)
    ));
    Diagnostic {
        code: "CAST_002".to_string(),
        statement: None,
        message: format!("numeric saturation when casting to {}", target_type),
        span: Some(span),
        label,
        help: Some(format!(
            "use a value within the valid range for {} or cast to a wider type",
            target_type
        )),
        notes: vec![format!("valid range for {}: {}", target_type, get_type_range(target_type))],
        column: None,
        caused_by: None,
    }
}

pub fn invalid_number(span: Span, target_type: DataType, cause: Diagnostic) -> Diagnostic {
    let label = Some(format!("failed to cast to {}", target_type));
    Diagnostic {
        code: "CAST_003".to_string(),
        statement: None,
        message: format!("failed to cast to {}", target_type),
        span: Some(span),
        label,
        help: None,
        notes: vec![],
        column: None,
        caused_by: Some(Box::from(cause)),
    }
}

pub fn invalid_temporal(span: Span, target_type: DataType, cause: Diagnostic) -> Diagnostic {
    let label = Some(format!("failed to cast to {}", target_type));
    Diagnostic {
        code: "CAST_004".to_string(),
        statement: None,
        message: format!("failed to cast to {}", target_type),
        span: Some(span),
        label,
        help: None,
        notes: vec![],
        column: None,
        caused_by: Some(Box::from(cause)),
    }
}

fn get_type_range(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Int1 => "-128 to 127",
        DataType::Int2 => "-32,768 to 32,767",
        DataType::Int4 => "-2,147,483,648 to 2,147,483,647",
        DataType::Int8 => "-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807",
        DataType::Int16 => {
            "-170,141,183,460,469,231,731,687,303,715,884,105,728 to 170,141,183,460,469,231,731,687,303,715,884,105,727"
        }
        DataType::Uint1 => "0 to 255",
        DataType::Uint2 => "0 to 65,535",
        DataType::Uint4 => "0 to 4,294,967,295",
        DataType::Uint8 => "0 to 18,446,744,073,709,551,615",
        DataType::Uint16 => "0 to 340,282,366,920,938,463,463,374,607,431,768,211,455",
        DataType::Float4 => "±3.4E+38 (7 digits precision)",
        DataType::Float8 => "±1.7E+308 (15 digits precision)",
        DataType::Bool => "true or false",
        _ => "see documentation",
    }
}
