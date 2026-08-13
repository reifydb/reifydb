// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::buffer::memory::MemoryOperatorStorage;

#[derive(Debug, Clone)]
pub enum OperatorBufferTier {
	Memory(MemoryOperatorStorage),
}

impl OperatorBufferTier {
	pub fn memory() -> Self {
		Self::Memory(MemoryOperatorStorage::new())
	}
}
