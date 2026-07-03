// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::encoded::shape::RowShape;

use super::{ringbuffer::RingBuffer, table::Table, view::View};
use crate::row::row_shape_from_columns;

pub trait GetShape {
	fn get_shape(&self) -> RowShape;
}

impl GetShape for Table {
	fn get_shape(&self) -> RowShape {
		row_shape_from_columns(&self.columns)
	}
}

impl GetShape for View {
	fn get_shape(&self) -> RowShape {
		row_shape_from_columns(self.columns())
	}
}

impl GetShape for RingBuffer {
	fn get_shape(&self) -> RowShape {
		row_shape_from_columns(&self.columns)
	}
}
