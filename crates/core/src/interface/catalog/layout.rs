// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::{ringbuffer::RingBuffer, table::Table, view::View};
use crate::encoded::schema::Schema;

pub trait GetSchema {
	fn get_schema(&self) -> Schema;
}

impl GetSchema for Table {
	fn get_schema(&self) -> Schema {
		Schema::from(&self.columns)
	}
}

impl GetSchema for View {
	fn get_schema(&self) -> Schema {
		Schema::from(self.columns())
	}
}

impl GetSchema for RingBuffer {
	fn get_schema(&self) -> Schema {
		Schema::from(&self.columns)
	}
}
