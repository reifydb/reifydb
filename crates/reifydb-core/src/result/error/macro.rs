// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

/// Macro to create an Error from a diagnostic function call
///
/// Usage:
/// - `error!(diagnostic_function(args))` - Creates an error without span
/// - `error!(diagnostic_function(args), span)` - Creates an error with span
///
/// Expands to: `Error(diagnostic_function(args))` or
/// `Error(diagnostic_function(args).with_span(span))`
///
/// Examples:
/// - `error!(sequence_exhausted(Type::Uint8))`
/// - `error!(sequence_exhausted(Type::Uint8), span)`
#[macro_export]
macro_rules! error {
	($diagnostic:expr) => {
		$crate::error::Error($diagnostic)
	};
	($diagnostic:expr, $span:expr) => {{
		let mut diag = $diagnostic;
		diag.with_span(&$crate::IntoOwnedSpan::into_span($span));
		$crate::error::Error(diag)
	}};
}

/// Macro to return an error from a diagnostic function call
///
/// Usage:
/// - `return_error!(diagnostic_function(args))` - Returns an error without span
/// - `return_error!(diagnostic_function(args), span)` - Returns an error with
///   span
///
/// Expands to: `return Err(Error(diagnostic_function(args)))` or `return
/// Err(Error(diagnostic_function(args).with_span(span)))`
///
/// Examples:
/// - `return_error!(sequence_exhausted(Type::Uint8))`
/// - `return_error!(sequence_exhausted(Type::Uint8), span)`
#[macro_export]
macro_rules! return_error {
	($diagnostic:expr) => {
		return Err($crate::error::Error($diagnostic))
	};
	($diagnostic:expr, $span:expr) => {{
		let mut diag = $diagnostic;
		diag.with_span(&$crate::IntoOwnedSpan::into_span($span));
		return Err($crate::error::Error(diag));
	}};
}

/// Macro to create an Err(Error()) from a diagnostic function call
///
/// Usage:
/// - `err!(diagnostic_function(args))` - Creates an Err without span
/// - `err!(diagnostic_function(args), span)` - Creates an Err with span
///
/// Expands to: `Err(Error(diagnostic_function(args)))` or
/// `Err(Error(diagnostic_function(args).with_span(span)))`
///
/// Examples:
/// - `err!(sequence_exhausted(Type::Uint8))`
/// - `err!(sequence_exhausted(Type::Uint8), span)`
#[macro_export]
macro_rules! err {
	($diagnostic:expr) => {
		Err($crate::error::Error($diagnostic))
	};
	($diagnostic:expr, $span:expr) => {{
		let mut diag = $diagnostic;
		diag.with_span(&$crate::IntoOwnedSpan::into_span($span));
		Err($crate::error::Error(diag))
	}};
}

#[cfg(test)]
mod tests {
	use crate::{
		OwnedSpan, SpanColumn, SpanLine, Type, err, error,
		result::error::diagnostic::sequence::sequence_exhausted,
		return_error,
	};

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
		let result: Result<(), crate::Error> =
			err!(sequence_exhausted(Type::Uint8));

		assert!(result.is_err());

		if let Err(err) = result {
			let diagnostic = err.diagnostic();
			assert!(diagnostic.message.contains("exhausted"));
		}
	}

	#[test]
	fn test_error_macro_with_span() {
		// Create a test span
		let span = OwnedSpan {
			line: SpanLine(42),
			column: SpanColumn(10),
			fragment: "test fragment".to_string(),
		};

		// Test that error! macro with span creates correct Error type
		let err = error!(sequence_exhausted(Type::Uint8), span.clone());

		// Verify it creates the correct Error type
		assert!(matches!(err, crate::Error(_)));

		// Test that the diagnostic has the span set
		let diagnostic = err.diagnostic();
		assert!(diagnostic.span.is_some());
		assert_eq!(diagnostic.span.as_ref().unwrap().line.0, 42);
		assert_eq!(diagnostic.span.as_ref().unwrap().column.0, 10);
	}

	#[test]
	fn test_return_error_macro_with_span() {
		fn test_fn() -> Result<(), crate::Error> {
			let span = OwnedSpan {
				line: SpanLine(100),
				column: SpanColumn(25),
				fragment: "error location".to_string(),
			};
			return_error!(sequence_exhausted(Type::Uint8), span);
		}

		let result = test_fn();
		assert!(result.is_err());

		if let Err(err) = result {
			let diagnostic = err.diagnostic();
			assert!(diagnostic.span.is_some());
			assert_eq!(
				diagnostic.span.as_ref().unwrap().line.0,
				100
			);
			assert_eq!(
				diagnostic.span.as_ref().unwrap().column.0,
				25
			);
		}
	}

	#[test]
	fn test_err_macro_with_span() {
		let span = OwnedSpan {
			line: SpanLine(200),
			column: SpanColumn(50),
			fragment: "err span test".to_string(),
		};

		// Test that err! macro with span creates correct Result type
		// with Err
		let result: Result<(), crate::Error> =
			err!(sequence_exhausted(Type::Uint8), span);

		assert!(result.is_err());

		if let Err(err) = result {
			let diagnostic = err.diagnostic();
			assert!(diagnostic.span.is_some());
			assert_eq!(
				diagnostic.span.as_ref().unwrap().line.0,
				200
			);
			assert_eq!(
				diagnostic.span.as_ref().unwrap().column.0,
				50
			);
		}
	}

	#[test]
	fn test_macros_with_closure_span() {
		// Test with closure that returns OwnedSpan (implements
		// IntoOwnedSpan)
		let get_span = || OwnedSpan {
			line: SpanLine(300),
			column: SpanColumn(75),
			fragment: "closure span".to_string(),
		};

		let err = error!(sequence_exhausted(Type::Uint8), get_span);
		let diagnostic = err.diagnostic();
		assert!(diagnostic.span.is_some());
		assert_eq!(diagnostic.span.as_ref().unwrap().line.0, 300);
	}
}
