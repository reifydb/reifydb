// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

/// Macro to create an Error from a diagnostic function call
///
/// Usage:
/// - `error!(diagnostic_function(args))` - Creates an error without fragment
/// - `error!(diagnostic_function(args), fragment)` - Creates an error with fragment
///
/// Expands to: `Error(diagnostic_function(args))` or
/// `Error(diagnostic_function(args).with_fragment(fragment))`
///
/// Examples:
/// - `error!(sequence_exhausted(Type::Uint8))`
/// - `error!(sequence_exhausted(Type::Uint8), fragment)`
#[macro_export]
macro_rules! error {
	($diagnostic:expr) => {
		$crate::Error($diagnostic)
	};
	($diagnostic:expr, $fragment:expr) => {{
		let mut diag = $diagnostic;
		diag.with_fragment($fragment.into());
		$crate::Error(diag)
	}};
}

/// Macro to return an error from a diagnostic function call
///
/// Usage:
/// - `return_error!(diagnostic_function(args))` - Returns an error without fragment
/// - `return_error!(diagnostic_function(args), fragment)` - Returns an error with fragment
///
/// Expands to: `return Err(Error(diagnostic_function(args)))` or `return
/// Err(Error(diagnostic_function(args).with_fragment(fragment)))`
///
/// Examples:
/// - `return_error!(sequence_exhausted(Type::Uint8))`
/// - `return_error!(sequence_exhausted(Type::Uint8), fragment)`
#[macro_export]
macro_rules! return_error {
	($diagnostic:expr) => {
		return Err($crate::Error($diagnostic))
	};
	($diagnostic:expr, $fragment:expr) => {{
		let mut diag = $diagnostic;
		diag.with_fragment($fragment.into());
		return Err($crate::Error(diag));
	}};
}

/// Macro to create an Err(Error()) from a diagnostic function call
///
/// Usage:
/// - `err!(diagnostic_function(args))` - Creates an Err without fragment
/// - `err!(diagnostic_function(args), fragment)` - Creates an Err with fragment
///
/// Expands to: `Err(Error(diagnostic_function(args)))` or
/// `Err(Error(diagnostic_function(args).with_fragment(fragment)))`
///
/// Examples:
/// - `err!(sequence_exhausted(Type::Uint8))`
/// - `err!(sequence_exhausted(Type::Uint8), fragment)`
#[macro_export]
macro_rules! err {
	($diagnostic:expr) => {
		Err($crate::Error($diagnostic))
	};
	($diagnostic:expr, $fragment:expr) => {{
		let mut diag = $diagnostic;
		diag.with_fragment($fragment.into());
		Err($crate::Error(diag))
	}};
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use crate::{Fragment, StatementColumn, StatementLine, Type, error::diagnostic::sequence::sequence_exhausted};

	#[test]
	fn test_error_macro() {
		// Test that error! macro creates correct Error type
		let err = error!(sequence_exhausted(Type::Uint8));

		// Verify it creates the correct Error type
		assert!(matches!(err, crate::Error(_)));

		// Test that the diagnostic is properly wrapped
		let diagnostic = err.diagnostic();
		assert!(diagnostic.message.contains("exhausted"));
	}

	#[test]
	fn test_return_error_macro() {
		fn test_fn() -> Result<(), crate::Error> {
			return_error!(sequence_exhausted(Type::Uint8));
		}

		let result = test_fn();
		assert!(result.is_err());

		if let Err(err) = result {
			let diagnostic = err.diagnostic();
			assert!(diagnostic.message.contains("exhausted"));
		}
	}

	#[test]
	fn test_err_macro() {
		// Test that err! macro creates correct Result type with Err
		let result: Result<(), crate::Error> = err!(sequence_exhausted(Type::Uint8));

		assert!(result.is_err());

		if let Err(err) = result {
			let diagnostic = err.diagnostic();
			assert!(diagnostic.message.contains("exhausted"));
		}
	}

	#[test]
	fn test_error_macro_with_fragment() {
		// Create a test fragment
		let fragment = Fragment::Statement {
			line: StatementLine(42),
			column: StatementColumn(10),
			text: Arc::new("test fragment".to_string()),
		};

		// Test that error! macro with fragment creates correct Error
		// type
		let err = error!(sequence_exhausted(Type::Uint8), fragment.clone());

		// Verify it creates the correct Error type
		assert!(matches!(err, crate::Error(_)));

		// Test that the diagnostic has the origin set (via fragment)
		let diagnostic = err.diagnostic();
		let fragment = diagnostic.fragment();
		assert!(fragment.is_some());
		if let Some(Fragment::Statement {
			line,
			column,
			..
		}) = fragment.as_ref()
		{
			assert_eq!(line.0, 42);
			assert_eq!(column.0, 10);
		}
	}

	#[test]
	fn test_return_error_macro_with_fragment() {
		fn test_fn() -> Result<(), crate::Error> {
			let fragment = Fragment::Statement {
				line: StatementLine(100),
				column: StatementColumn(25),
				text: Arc::new("error location".to_string()),
			};
			return_error!(sequence_exhausted(Type::Uint8), fragment);
		}

		let result = test_fn();
		assert!(result.is_err());

		if let Err(err) = result {
			let diagnostic = err.diagnostic();
			let fragment = diagnostic.fragment();
			assert!(fragment.is_some());
			if let Some(Fragment::Statement {
				line,
				column,
				..
			}) = fragment.as_ref()
			{
				assert_eq!(line.0, 100);
				assert_eq!(column.0, 25);
			}
		}
	}

	#[test]
	fn test_err_macro_with_fragment() {
		let fragment = Fragment::Statement {
			line: StatementLine(200),
			column: StatementColumn(50),
			text: Arc::new("err fragment test".to_string()),
		};

		// Test that err! macro with fragment creates correct Result
		// type with Err
		let result: Result<(), crate::Error> = err!(sequence_exhausted(Type::Uint8), fragment);

		assert!(result.is_err());

		if let Err(err) = result {
			let diagnostic = err.diagnostic();
			let fragment = diagnostic.fragment();
			assert!(fragment.is_some());
			if let Some(Fragment::Statement {
				line,
				column,
				..
			}) = fragment.as_ref()
			{
				assert_eq!(line.0, 200);
				assert_eq!(column.0, 50);
			}
		}
	}

	#[test]
	fn test_macros_with_closure_fragment() {
		// Test with closure that returns Fragment (implements
		// Into<Fragment>)
		let get_fragment = || Fragment::Statement {
			line: StatementLine(300),
			column: StatementColumn(75),
			text: Arc::new("closure fragment".to_string()),
		};

		let err = error!(sequence_exhausted(Type::Uint8), get_fragment());
		let diagnostic = err.diagnostic();
		let fragment = diagnostic.fragment();
		assert!(fragment.is_some());
		assert_eq!(fragment.as_ref().unwrap().line().0, 300);
	}
}
