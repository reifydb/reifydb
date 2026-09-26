// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::interface::{
	catalog::{
		dictionary::Dictionary,
		id::{TableId, ViewId},
		object::ObjectId,
		table::Table,
		view::View,
	},
	change::Diff,
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_value::{
	Result,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

pub trait Changes {
	fn cursor(&self) -> usize;

	fn entries_from(&self, at: usize) -> Vec<(ObjectId, Diff)>;

	fn set_cursor(&mut self, at: usize);
}

pub trait Rows {
	fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>>;

	fn set(&mut self, key: &EncodedKey, row: EncodedBytes) -> Result<()>;

	fn remove(&mut self, key: &EncodedKey) -> Result<()>;
}

pub trait Emit {
	fn emit(&mut self, view: ViewId, diff: Diff) -> Result<()>;
}

pub trait Lookup {
	fn transactional_flows(&mut self) -> Result<Vec<FlowDag>>;

	fn view(&mut self, id: ViewId) -> Result<View>;

	fn table(&mut self, id: TableId) -> Result<Table>;

	fn dictionary(&mut self, id: DictionaryId) -> Result<Dictionary>;
}

pub trait Intern {
	fn intern(&mut self, dictionary: &Dictionary, value: &Value) -> Result<DictionaryEntryId>;

	fn resolve(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>>;
}

pub trait ClockNow {
	fn now(&self) -> DateTime;
}
