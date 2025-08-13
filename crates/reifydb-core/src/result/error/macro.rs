// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

/// Macro to create an Error from a diagnostic function call
///
/// Usage: `error!(diagnostic_function(args))`
/// Expands to: `Error(diagnostic_function(args))`
///
/// Example: `error!(sequence_exhausted(Type::Uint8))`
#[macro_export]
macro_rules! error {
	($diagnostic:expr) => {
		$crate::error::Error($diagnostic)
	};
}

/// Macro to return an error from a diagnostic function call
///
/// Usage: `return_error!(diagnostic_function(args))`
/// Expands to: `return Err(Error(diagnostic_function(args)))`
///
/// Example: `return_error!(sequence_exhausted(Type::Uint8))`
#[macro_export]
macro_rules! return_error {
	($diagnostic:expr) => {
		return Err($crate::error::Error($diagnostic))
	};
}

/// Macro to create an Err(Error()) from a diagnostic function call
///
/// Usage: `err!(diagnostic_function(args))`
/// Expands to: `Err(Error(diagnostic_function(args)))`
///
/// Example: `err!(sequence_exhausted(Type::Uint8))`
#[macro_export]
macro_rules! err {
	($diagnostic:expr) => {
		Err($crate::error::Error($diagnostic))
	};
}

#[cfg(test)]
mod tests {
	use crate::{
		Type, err, error,
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
}
