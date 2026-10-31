// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EnumVariant {
	pub r#enum: String,
	pub variant: String,
}

impl EnumVariant {
	pub fn new(r#enum: impl Into<String>, variant: impl Into<String>) -> Self {
		Self {
			r#enum: r#enum.into(),
			variant: variant.into(),
		}
	}
}

impl Display for EnumVariant {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.write_str(&self.variant)
	}
}
