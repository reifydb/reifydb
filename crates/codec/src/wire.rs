// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::frame::frame::Frame;
use serde::{Deserialize, Serialize};

use crate::{error::DecodeError, frame::decode::decode_frames};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WireFormat {
	#[default]
	Frames,
	Rbcf,
}

#[derive(Debug, Clone)]
pub enum RawChangePayload {
	Rbcf(Vec<u8>),
	Empty,
}

impl RawChangePayload {
	pub fn into_frames(self) -> Result<Vec<Frame>, DecodeError> {
		match self {
			Self::Rbcf(bytes) => decode_frames(&bytes),
			Self::Empty => Ok(Vec::new()),
		}
	}
}
