// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::module::value::Value;

#[derive(Clone)]
pub struct Global {
	pub mutable: bool,
	pub value: Value,
}
