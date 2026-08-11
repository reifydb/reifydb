// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::data::buffer::ExternCBuffer;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCRowShapeField {
	pub name: ExternCBuffer,

	pub base_type: u8,

	pub constraint_type: u8,

	pub constraint_param1: u32,

	pub constraint_param2: u32,

	pub offset: u32,

	pub size: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ExternCRowShape {
	pub fingerprint: u64,

	pub family: u8,

	pub fields: *const ExternCRowShapeField,

	pub field_count: usize,
}
