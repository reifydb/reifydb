// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	error,
	fmt::{self, Display, Formatter},
};

use super::frame::Frame;
use crate::value::try_from::FromValueError;

#[derive(Debug, Clone)]
pub enum FromFrameError {
	MissingColumn {
		column: String,
		struct_name: &'static str,
	},

	ValueError {
		column: String,
		row: usize,
		error: FromValueError,
	},
}

impl Display for FromFrameError {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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

impl error::Error for FromFrameError {}

pub trait FromFrame: Sized {
	fn from_frame(frame: &Frame) -> Result<Vec<Self>, FromFrameError>;
}
