// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::common::extern_c::wire::columns::ExternCColumns;

#[repr(C)]
pub struct ExternCSinkRecord {
	pub op: u8,

	pub columns: ExternCColumns,
}
