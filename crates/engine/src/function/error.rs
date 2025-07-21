// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// Re-export core::Error as the unified error type for this module
pub use reifydb_core::Error;

// Helper functions to create specific function errors
use reifydb_core::diagnostic::Diagnostic;
use reifydb_core::Type;

pub fn unknown_function(name: String) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "FN_001".to_string(),
        statement: None,
        message: format!("Unknown function: {}", name),
        column: None,
        span: None,
        label: None,
        help: Some("Check function name spelling or available functions".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn arity_mismatch(function: String, expected: usize, actual: usize) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "FN_002".to_string(),
        statement: None,
        message: format!("Function '{}' expects {} arguments, got {}", function, expected, actual),
        column: None,
        span: None,
        label: None,
        help: Some(format!("Provide exactly {} arguments", expected)),
        notes: vec![],
        cause: None,
    })
}

pub fn too_many_arguments(function: String, max_args: usize, actual: usize) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "FN_003".to_string(),
        statement: None,
        message: format!("Function '{}' accepts at most {} arguments, got {}", function, max_args, actual),
        column: None,
        span: None,
        label: None,
        help: Some(format!("Provide at most {} arguments", max_args)),
        notes: vec![],
        cause: None,
    })
}

pub fn invalid_argument_type(function: String, index: usize, expected_one_of: Vec<Type>, actual: Type) -> reifydb_core::Error {
    let expected_str = expected_one_of.iter()
        .map(|t| format!("{:?}", t))
        .collect::<Vec<_>>()
        .join(", ");
    
    reifydb_core::Error(Diagnostic {
        code: "FN_004".to_string(),
        statement: None,
        message: format!("Function '{}' argument {} has invalid type", function, index + 1),
        column: None,
        span: None,
        label: None,
        help: Some(format!("Expected one of: {}, got: {:?}", expected_str, actual)),
        notes: vec![],
        cause: None,
    })
}

pub fn undefined_argument(function: String, index: usize) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "FN_005".to_string(),
        statement: None,
        message: format!("Function '{}' argument {} is undefined", function, index + 1),
        column: None,
        span: None,
        label: None,
        help: Some("Ensure all arguments are properly defined".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn missing_input(function: String) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "FN_006".to_string(),
        statement: None,
        message: format!("Function '{}' is missing required input", function),
        column: None,
        span: None,
        label: None,
        help: Some("Provide the required input data".to_string()),
        notes: vec![],
        cause: None,
    })
}

pub fn execution_failed(function: String, reason: String) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "FN_007".to_string(),
        statement: None,
        message: format!("Function '{}' execution failed: {}", function, reason),
        column: None,
        span: None,
        label: None,
        help: None,
        notes: vec![],
        cause: None,
    })
}

pub fn internal_error(function: String, details: String) -> reifydb_core::Error {
    reifydb_core::Error(Diagnostic {
        code: "FN_008".to_string(),
        statement: None,
        message: format!("Internal error in function '{}': {}", function, details),
        column: None,
        span: None,
        label: None,
        help: Some("This is likely a bug, please report it".to_string()),
        notes: vec![],
        cause: None,
    })
}