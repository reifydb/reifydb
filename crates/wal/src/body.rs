// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::error::Result;

pub trait Body: Sized {
	fn kind(&self) -> u32;

	fn encode(&self, out: &mut Vec<u8>);

	fn decode(kind: u32, bytes: &[u8]) -> Result<Self>;
}
