// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_value::error::ValueError;

use crate::vm::vm::Vm;

impl<'a> Vm<'a> {
	pub(crate) fn pop_as_column(&mut self) -> Result<(FieldRef, ArrayRef), ValueError> {
		self.stack.pop()?.into_column()
	}
}
