// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::module::value::Value;

#[derive(Clone)]
pub struct Global {
	pub mutable: bool,
	pub value: Value,
}
