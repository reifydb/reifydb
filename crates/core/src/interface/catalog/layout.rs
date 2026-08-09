// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShape};

use super::{ringbuffer::RingBuffer, table::Table, view::View};
use crate::row::row_shape_from_columns;

pub trait GetRowShape {
	fn get_row_shape(&self) -> RowShape;
}

impl GetRowShape for Table {
	fn get_row_shape(&self) -> RowShape {
		row_shape_from_columns(RowFamily::Table, &self.columns)
	}
}

impl GetRowShape for View {
	fn get_row_shape(&self) -> RowShape {
		row_shape_from_columns(self.storage_kind().row_family(), self.columns())
	}
}

impl GetRowShape for RingBuffer {
	fn get_row_shape(&self) -> RowShape {
		row_shape_from_columns(RowFamily::RingBuffer, &self.columns)
	}
}
