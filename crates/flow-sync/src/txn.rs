// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	flow::dag::FlowDag,
	interface::{
		catalog::{
			id::{TableId, ViewId},
			object::ObjectId,
			table::Table,
			view::View,
		},
		change::Diff,
	},
};
use reifydb_value::{Result, value::datetime::DateTime};

pub trait Changes {
	fn cursor(&self) -> usize;

	fn entries_from(&self, at: usize) -> &[(ObjectId, Diff)];

	fn set_cursor(&mut self, at: usize);
}

pub trait Lookup {
	fn transactional_flows(&mut self) -> Result<Vec<FlowDag>>;

	fn view(&mut self, id: ViewId) -> Result<View>;

	fn table(&mut self, id: TableId) -> Result<Table>;
}

pub trait ClockNow {
	fn now(&self) -> DateTime;
}
