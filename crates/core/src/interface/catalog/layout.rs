// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::{ringbuffer::RingBufferDef, table::TableDef, view::ViewDef};
use crate::encoded::schema::Schema;

pub trait GetSchema {
	fn get_schema(&self) -> Schema;
}

impl GetSchema for TableDef {
	fn get_schema(&self) -> Schema {
		Schema::from(&self.columns)
	}
}

impl GetSchema for ViewDef {
	fn get_schema(&self) -> Schema {
		Schema::from(&self.columns)
	}
}

impl GetSchema for RingBufferDef {
	fn get_schema(&self) -> Schema {
		Schema::from(&self.columns)
	}
}
