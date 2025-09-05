// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{Error, return_error};

/// Precision for a decimal type (1-38 total digits)
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
pub struct Precision(u8);

impl Precision {
	/// Create a new Precision value
	///
	/// # Panics
	/// Panics if precision is 0 or greater than 38
	pub fn new(precision: u8) -> Self {
		assert!(
			precision > 0 && precision <= 38,
			"Precision must be between 1 and 38, got {}",
			precision
		);
		Self(precision)
	}

	/// Create a new Precision value, returning an error if invalid
	pub fn try_new(precision: u8) -> Result<Self, Error> {
		if precision == 0 || precision > 38 {
			use crate::error::diagnostic::Diagnostic;
			return_error!(Diagnostic {
				code: "NUMBER_006".to_string(),
				statement: None,
				message: "invalid decimal precision".to_string(),
				fragment: crate::OwnedFragment::None,
				label: Some(format!("precision ({}) must be between 1 and 38", precision)),
				help: Some("use a precision value between 1 and 38".to_string()),
				notes: vec![
					format!("current precision: {}", precision),
					"precision represents the total number of significant digits".to_string(),
					"compatible range: 1 to 38".to_string(),
				],
				column: None,
				cause: None,
			});
		}
		Ok(Self(precision))
	}

	/// Get the precision value
	pub fn value(self) -> u8 {
		self.0
	}

	/// Maximum precision (38)
	pub const MAX: Self = Self(38);

	/// Minimum precision (1)
	pub const MIN: Self = Self(1);
}

impl Display for Precision {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		self.0.fmt(f)
	}
}

impl From<Precision> for u8 {
	fn from(precision: Precision) -> Self {
		precision.0
	}
}

impl From<u8> for Precision {
	fn from(value: u8) -> Self {
		Self::new(value)
	}
}
