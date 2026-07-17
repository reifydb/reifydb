// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum WireFormat {
	Json,
	#[default]
	Frames,
	Rbcf,
}

impl<'de> Deserialize<'de> for WireFormat {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let raw = String::deserialize(deserializer)?;
		match raw.to_ascii_lowercase().as_str() {
			"json" => Ok(WireFormat::Json),
			"frames" => Ok(WireFormat::Frames),
			"rbcf" => Ok(WireFormat::Rbcf),
			other => Err(D::Error::custom(format!(
				"unknown wire format `{other}`: expected json, frames, or rbcf"
			))),
		}
	}
}
