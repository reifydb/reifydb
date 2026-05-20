// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use super::{ringbuffer::RingBuffer, table::Table, view::View};
use crate::encoded::shape::RowShape;

pub trait GetShape {
	fn get_shape(&self) -> RowShape;
}

impl GetShape for Table {
	fn get_shape(&self) -> RowShape {
		RowShape::from(&self.columns)
	}
}

impl GetShape for View {
	fn get_shape(&self) -> RowShape {
		RowShape::from(self.columns())
	}
}

impl GetShape for RingBuffer {
	fn get_shape(&self) -> RowShape {
		RowShape::from(&self.columns)
	}
}
