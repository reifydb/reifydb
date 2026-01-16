// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::fmt::{Display, Formatter};

use super::frame::Frame;
use crate::value::try_from::FromValueError;

/// Error type for Frame deserialization via `FromFrame` derive.
#[derive(Debug, Clone)]
pub enum FromFrameError {
	/// A required column was not found in the frame.
	MissingColumn {
		column: String,
		struct_name: &'static str,
	},
	/// Value extraction failed for a specific cell.
	ValueError {
		column: String,
		row: usize,
		error: FromValueError,
	},
}

impl Display for FromFrameError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			FromFrameError::MissingColumn {
				column,
				struct_name,
			} => {
				write!(f, "missing column '{}' required by struct '{}'", column, struct_name)
			}
			FromFrameError::ValueError {
				column,
				row,
				error,
			} => {
				write!(f, "error extracting column '{}' row {}: {}", column, row, error)
			}
		}
	}
}

impl std::error::Error for FromFrameError {}

/// Trait for types that can be deserialized from a Frame.
///
/// This trait is typically derived using `#[derive(FromFrame)]` from the
/// `reifydb` or `reifydb_client` crates.
///
/// # Example
///
/// ```ignore
/// use reifydb::FromFrame;  // or reifydb_client::FromFrame
///
/// #[derive(FromFrame)]
/// struct User {
///     id: i64,
///     name: String,
/// }
///
/// let users: Vec<User> = User::from_frame(&frame)?;
/// // or using TryFrom:
/// let users: Vec<User> = (&frame).try_into()?;
/// ```
pub trait FromFrame: Sized {
	/// Deserialize a vector of Self from a Frame.
	fn from_frame(frame: &Frame) -> Result<Vec<Self>, FromFrameError>;
}
