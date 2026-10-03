// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	flow::dag::FlowDag,
	interface::{
		catalog::{
			dictionary::Dictionary,
			id::{TableId, ViewId},
			object::ObjectId,
			table::Table,
			view::View,
		},
		change::Diff,
	},
	internal_err,
};
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

	fn entries_from(&self, at: usize) -> &[(ObjectId, Diff)];

	fn set_cursor(&mut self, at: usize);
}

pub trait Rows {
	fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>>;

	fn set(&mut self, key: &EncodedKey, row: EncodedBytes) -> Result<()>;

	fn remove(&mut self, key: &EncodedKey) -> Result<()>;

	fn get_many(&mut self, keys: &[EncodedKey]) -> Result<Vec<Option<EncodedBytes>>> {
		keys.iter().map(|key| self.get(key)).collect()
	}

	fn set_many(&mut self, keys: &[EncodedKey], rows: Vec<EncodedBytes>) -> Result<()> {
		if keys.len() != rows.len() {
			return internal_err!("set_many got {} keys and {} rows", keys.len(), rows.len());
		}
		for (key, row) in keys.iter().zip(rows) {
			self.set(key, row)?;
		}
		Ok(())
	}

	fn remove_many(&mut self, keys: &[EncodedKey]) -> Result<()> {
		for key in keys {
			self.remove(key)?;
		}
		Ok(())
	}
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

	fn find(&mut self, dictionary: &Dictionary, value: &Value) -> Result<Option<DictionaryEntryId>>;

	fn resolve(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>>;
}

pub trait ClockNow {
	fn now(&self) -> DateTime;
}
