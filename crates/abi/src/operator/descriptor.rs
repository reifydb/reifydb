// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{column::OperatorColumnsFFI, vtable::OperatorVTableFFI};
use crate::data::buffer::BufferFFI;

#[repr(C)]
pub struct OperatorDescriptorFFI {
	pub api: u32,

	pub operator: BufferFFI,

	pub version: BufferFFI,

	pub description: BufferFFI,

	pub input_columns: OperatorColumnsFFI,

	pub output_columns: OperatorColumnsFFI,

	pub capabilities: u32,

	pub vtable: OperatorVTableFFI,
}

// SAFETY: OperatorDescriptorFFI contains pointers to static strings and functions

unsafe impl Send for OperatorDescriptorFFI {}
unsafe impl Sync for OperatorDescriptorFFI {}
