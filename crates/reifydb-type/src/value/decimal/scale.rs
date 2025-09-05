// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use super::precision::Precision;
use crate::{Error, return_error};

/// Scale for a decimal type (0 to precision decimal places)
#[derive(
	Clone,
	Copy,
	Debug,
	PartialEq,
	Eq,
	Hash,
	PartialOrd,
	Ord,
	Serialize,
	Deserialize,
)]
#[repr(transparent)]
pub struct Scale(u8);

impl Scale {
	/// Create a new Scale value
	///
	/// # Panics
	/// Panics if scale is greater than 38
	pub fn new(scale: u8) -> Self {
		assert!(
			scale <= 38,
			"Scale must be between 0 and 38, got {}",
			scale
		);
		Self(scale)
	}

	/// Create a new Scale value, returning an error if invalid
	pub fn try_new(scale: u8) -> Result<Self, Error> {
		if scale > 38 {
			use crate::error::diagnostic::Diagnostic;
			return_error!(Diagnostic {
				code: "NUMBER_007".to_string(),
				statement: None,
				message: "invalid decimal scale".to_string(),
				fragment: crate::OwnedFragment::None,
				label: Some(format!("scale ({}) must be between 0 and 38", scale)),
				help: Some("use a scale value between 0 and 38".to_string()),
				notes: vec![
					format!("current scale: {}", scale),
					"scale represents the number of digits after the decimal point".to_string(),
					"compatible range: 0 to 38".to_string(),
				],
				column: None,
				cause: None,
			});
		}
		Ok(Self(scale))
	}

	/// Create a new Scale value with validation against precision
	pub fn try_new_with_precision(
		scale: u8,
		precision: Precision,
	) -> Result<Self, Error> {
		if scale > precision.value() {
			use crate::error::diagnostic::Diagnostic;
			return_error!(Diagnostic {
				code: "NUMBER_005".to_string(),
				statement: None,
				message: "decimal scale exceeds precision".to_string(),
				fragment: crate::OwnedFragment::None,
				label: Some(format!("scale ({}) cannot be greater than precision ({})", scale, precision.value())),
				help: Some(format!("use a scale value between 0 and {} or increase precision", precision.value())),
				notes: vec![
					format!("current precision: {}", precision.value()),
					format!("current scale: {}", scale),
					"scale represents the number of digits after the decimal point".to_string(),
					"precision represents the total number of significant digits".to_string(),
				],
				column: None,
				cause: None,
			});
		}
		if scale > 38 {
			use crate::error::diagnostic::Diagnostic;
			return_error!(Diagnostic {
				code: "NUMBER_007".to_string(),
				statement: None,
				message: "invalid decimal scale".to_string(),
				fragment: crate::OwnedFragment::None,
				label: Some(format!("scale ({}) must be between 0 and 38", scale)),
				help: Some("use a scale value between 0 and 38".to_string()),
				notes: vec![
					format!("current scale: {}", scale),
					"scale represents the number of digits after the decimal point".to_string(),
					"compatible range: 0 to 38".to_string(),
				],
				column: None,
				cause: None,
			});
		}
		Ok(Self(scale))
	}

	/// Get the scale value
	pub fn value(self) -> u8 {
		self.0
	}

	/// Maximum scale (38)
	pub const MAX: Self = Self(38);

	/// Minimum scale (0)
	pub const MIN: Self = Self(0);
}

impl Display for Scale {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		self.0.fmt(f)
	}
}

impl From<Scale> for u8 {
	fn from(scale: Scale) -> Self {
		scale.0
	}
}

impl From<u8> for Scale {
	fn from(value: u8) -> Self {
		Self::new(value)
	}
}
