// Copyright (c) 2026 ReifyDB
// SPDX-License-Identifier: Apache-2.0

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker

use std::any::Any;

use crate::log::{Entry, Index};

pub mod apply;
pub mod testing;

pub trait State: Send {
	fn get_applied_index(&self) -> Index;

	fn apply(&mut self, entry: &Entry);

	fn as_any(&self) -> &dyn Any;
}
