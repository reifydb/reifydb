// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::function::FunctionMode;
use reifydb_core::ValueKind;
use std::fmt;
use std::fmt::Display;

#[derive(Debug, Clone)]
pub enum FunctionError {
    /// The function name is not registered.
    UnknownFunction(String),

    /// Incorrect number of arguments.
    ArityMismatch { function: String, expected: usize, actual: usize },

    /// Function is not variadic but received too many arguments.
    TooManyArguments { function: String, max_args: usize, actual: usize },

    /// Argument has the wrong type.
    InvalidArgumentType {
        function: String,
        index: usize,
        expected_one_of: Vec<ValueKind>,
        actual: ValueKind,
    },

    /// One or more arguments are undefined when the function doesn't accept them.
    UndefinedArgument { function: String, index: usize },

    /// The function is being used in an unsupported context.
    UnsupportedMode { function: String, mode: FunctionMode },

    /// Function requires input rows but received none.
    MissingInput { function: String },

    /// Internal or user-defined function evaluation failed.
    ExecutionFailed { function: String, reason: String },

    /// Generic internal error.
    Internal { function: String, details: String },
}

impl Display for FunctionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use FunctionError::*;

        match self {
            UnknownFunction(name) => write!(f, "unknown function: '{}'", name),

            ArityMismatch { function, expected, actual } => write!(
                f,
                "function '{}' expects {} arguments, but got {}",
                function, expected, actual
            ),

            TooManyArguments { function, max_args, actual } => write!(
                f,
                "function '{}' only accepts up to {} arguments (got {})",
                function, max_args, actual
            ),

            InvalidArgumentType { function, index, expected_one_of, actual } => {
                if expected_one_of.len() == 1 {
                    write!(
                        f,
                        "function '{}' {} argument has unexpected type: expected {}, got {}",
                        function,
                        ordinal(*index),
                        expected_one_of[0],
                        actual
                    )
                } else {
                    write!(
                        f,
                        "function '{}' {} argument has unexpected type: expected one of [{}], got {}",
                        function,
                        ordinal(*index),
                        expected_one_of
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", "),
                        actual
                    )
                }
            }

            UndefinedArgument { function, index } => {
                write!(
                    f,
                    "function '{}' does not accept undefined for the {} argument",
                    function,
                    ordinal(*index)
                )
            }

            UnsupportedMode { function, mode } => {
                write!(f, "function '{}' does not support mode: {}", function, mode)
            }

            MissingInput { function } => {
                write!(f, "function '{}' requires input rows but none were provided", function)
            }

            ExecutionFailed { function, reason } => {
                write!(f, "function '{}' evaluation failed: {}", function, reason)
            }

            Internal { function, details } => {
                write!(f, "internal error in function '{}': {}", function, details)
            }
        }
    }
}

fn ordinal(index: usize) -> String {
    debug_assert!(
        index <= 20,
        "unlikely that a function will ever be called with more than 20 parameters"
    );
    match index + 1 {
        1 => "1st".to_string(),
        2 => "2nd".to_string(),
        3 => "3rd".to_string(),
        n => format!("{}th", n),
    }
}

impl std::error::Error for FunctionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::FunctionMode;

    #[test]
    fn test_unknown_function_display() {
        let err = FunctionError::UnknownFunction("foo".to_string());
        assert_eq!(err.to_string(), "unknown function: 'foo'");
    }

    #[test]
    fn test_arity_mismatch_display() {
        let err =
            FunctionError::ArityMismatch { function: "add".to_string(), expected: 2, actual: 3 };
        assert_eq!(err.to_string(), "function 'add' expects 2 arguments, but got 3");
    }

    #[test]
    fn test_too_many_arguments_display() {
        let err =
            FunctionError::TooManyArguments { function: "sum".to_string(), max_args: 5, actual: 7 };
        assert_eq!(err.to_string(), "function 'sum' only accepts up to 5 arguments (got 7)");
    }

    #[test]
    fn test_invalid_argument_type_single_expected_display() {
        let err = FunctionError::InvalidArgumentType {
            function: "sqrt".to_string(),
            index: 4,
            expected_one_of: vec![ValueKind::Int2],
            actual: ValueKind::String,
        };
        assert_eq!(
            err.to_string(),
            "function 'sqrt' 5th argument has unexpected type: expected INT2, got STRING"
        );
    }

    #[test]
    fn test_invalid_argument_type_multiple_expected_display() {
        let err = FunctionError::InvalidArgumentType {
            function: "if".to_string(),
            index: 1,
            expected_one_of: vec![ValueKind::Bool, ValueKind::Int2],
            actual: ValueKind::String,
        };
        assert_eq!(
            err.to_string(),
            "function 'if' 2nd argument has unexpected type: expected one of [BOOL, INT2], got STRING"
        );
    }

    #[test]
    fn test_undefined_argument_display() {
        let err = FunctionError::UndefinedArgument { function: "max".to_string(), index: 2 };
        assert_eq!(
            err.to_string(),
            "function 'max' does not accept undefined for the 3rd argument"
        );
    }

    #[test]
    fn test_unsupported_mode_display() {
        let err = FunctionError::UnsupportedMode {
            function: "rownum".to_string(),
            mode: FunctionMode::Aggregate,
        };
        assert_eq!(err.to_string(), "function 'rownum' does not support mode: Aggregate");
    }

    #[test]
    fn test_missing_input_display() {
        let err = FunctionError::MissingInput { function: "sum".to_string() };
        assert_eq!(err.to_string(), "function 'sum' requires input rows but none were provided");
    }

    #[test]
    fn test_execution_failed_display() {
        let err = FunctionError::ExecutionFailed {
            function: "parse_json".to_string(),
            reason: "unexpected EOF".to_string(),
        };
        assert_eq!(err.to_string(), "function 'parse_json' evaluation failed: unexpected EOF");
    }

    #[test]
    fn test_internal_error_display() {
        let err = FunctionError::Internal {
            function: "median".to_string(),
            details: "index out of bounds".to_string(),
        };
        assert_eq!(err.to_string(), "internal error in function 'median': index out of bounds");
    }

    #[test]
    fn test_ordinal_0_index() {
        assert_eq!(ordinal(0), "1st");
    }

    #[test]
    fn test_ordinal_1_index() {
        assert_eq!(ordinal(1), "2nd");
    }

    #[test]
    fn test_ordinal_2_index() {
        assert_eq!(ordinal(2), "3rd");
    }

    #[test]
    fn test_ordinal_3_index() {
        assert_eq!(ordinal(3), "4th");
    }

    #[test]
    fn test_ordinal_large_index() {
        assert_eq!(ordinal(10), "11th");
        assert_eq!(ordinal(19), "20th");
    }
}
