// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	num::NonZeroU64,
	ops::Bound::{Excluded, Unbounded},
	sync::Arc,
};

use arrow_array::RecordBatch;
use reifydb_core::{
	common::{CommitVersion, TimeSource},
	interface::catalog::object::ObjectId,
	value::{
		batch::batch,
		column::{
			builder::ColumnBuilder,
			factory::{datetime, uint8},
		},
	},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{
	Result,
	value::{
		Value,
		datetime::DateTime,
		row_number::RowNumber,
		system_columns::{SystemColumn, with_system_column},
		value_type::ValueType,
	},
};

use crate::backfill::Scan;

#[derive(Clone, Default)]
pub struct MemorySources(Arc<Mutex<Store>>);

struct Store {
	sources: BTreeMap<ObjectId, Source>,
	latest: CommitVersion,
	sealed: CommitVersion,
}

impl Default for Store {
	fn default() -> Self {
		Self {
			sources: BTreeMap::new(),
			latest: CommitVersion(0),
			sealed: CommitVersion(0),
		}
	}
}

struct Source {
	columns: Vec<(String, ValueType)>,
	time: TimeSource,
	next_row: u64,
	rows: BTreeMap<RowNumber, Vec<Write>>,
}

struct Write {
	version: CommitVersion,
	row: Option<StoredRow>,
}

#[derive(Clone)]
struct StoredRow {
	values: Vec<Value>,
	created_at: DateTime,
	updated_at: DateTime,
	time: Option<DateTime>,
}

struct Visible<'a> {
	row_number: RowNumber,
	version: CommitVersion,
	row: &'a StoredRow,
}

impl MemorySources {
	pub fn define(&self, source: ObjectId, columns: &[(&str, ValueType)], time: TimeSource) {
		let mut store = self.0.lock();
		assert!(
			!store.sources.contains_key(&source),
			"source {source:?} is already defined in the memory sources"
		);
		store.sources.insert(
			source,
			Source {
				columns: columns.iter().map(|(name, ty)| (name.to_string(), ty.clone())).collect(),
				time,
				next_row: 1,
				rows: BTreeMap::new(),
			},
		);
	}

	pub fn insert(&self, source: ObjectId, version: CommitVersion, at: DateTime, values: Vec<Value>) -> RowNumber {
		let mut store = self.0.lock();
		store.admit(version);
		let target = store.source_mut(source);
		target.check_width(source, &values);
		let row_number = RowNumber(target.next_row);
		target.next_row += 1;
		let time = match &target.time {
			TimeSource::None => None,
			TimeSource::Processing => Some(at),
			TimeSource::Event {
				ts,
			} => Some(target.event_time(source, ts, &values)),
		};
		target.write(
			row_number,
			version,
			Some(StoredRow {
				values,
				created_at: at,
				updated_at: at,
				time,
			}),
		);
		row_number
	}

	pub fn update(
		&self,
		source: ObjectId,
		row: RowNumber,
		version: CommitVersion,
		at: DateTime,
		values: Vec<Value>,
	) {
		let mut store = self.0.lock();
		store.admit(version);
		let target = store.source_mut(source);
		target.check_width(source, &values);
		let prior = target.live(source, row);
		let time = match &target.time {
			TimeSource::None => None,
			TimeSource::Processing => prior.time,
			TimeSource::Event {
				ts,
			} => Some(target.event_time(source, ts, &values)),
		};
		target.write(
			row,
			version,
			Some(StoredRow {
				values,
				created_at: prior.created_at,
				updated_at: at,
				time,
			}),
		);
	}

	pub fn delete(&self, source: ObjectId, row: RowNumber, version: CommitVersion) {
		let mut store = self.0.lock();
		store.admit(version);
		let target = store.source_mut(source);
		target.live(source, row);
		target.write(row, version, None);
	}

	pub fn latest(&self) -> CommitVersion {
		self.0.lock().latest
	}

	pub fn scan_at(&self, version: CommitVersion) -> MemoryScan {
		let mut store = self.0.lock();
		store.sealed = store.sealed.max(version);
		MemoryScan {
			sources: self.clone(),
			version,
			cursor: None,
		}
	}
}

impl Store {
	fn admit(&mut self, version: CommitVersion) {
		assert!(
			version >= self.latest,
			"write at version {} is older than the latest version {} of the memory sources",
			version.0,
			self.latest.0
		);
		assert!(
			version > self.sealed,
			"write at version {} lands at or below version {} that a scan already reads",
			version.0,
			self.sealed.0
		);
		self.latest = version;
	}

	fn source(&self, source: ObjectId) -> &Source {
		self.sources
			.get(&source)
			.unwrap_or_else(|| panic!("source {source:?} is not defined in the memory sources"))
	}

	fn source_mut(&mut self, source: ObjectId) -> &mut Source {
		self.sources
			.get_mut(&source)
			.unwrap_or_else(|| panic!("source {source:?} is not defined in the memory sources"))
	}
}

impl Source {
	fn check_width(&self, source: ObjectId, values: &[Value]) {
		assert_eq!(
			values.len(),
			self.columns.len(),
			"a row of {} values does not fit the {} columns of source {source:?}",
			values.len(),
			self.columns.len()
		);
	}

	fn event_time(&self, source: ObjectId, ts: &str, values: &[Value]) -> DateTime {
		let index =
			self.columns.iter().position(|(name, _)| name == ts).unwrap_or_else(|| {
				panic!("event time column {ts} is not a column of source {source:?}")
			});
		match &values[index] {
			Value::DateTime(time) => *time,
			found => panic!("event time column {ts} of source {source:?} holds {found:?}, not a datetime"),
		}
	}

	fn live(&self, source: ObjectId, row: RowNumber) -> StoredRow {
		self.rows
			.get(&row)
			.and_then(|history| history.last())
			.and_then(|write| write.row.clone())
			.unwrap_or_else(|| panic!("row {} of source {source:?} is not live", row.0))
	}

	fn write(&mut self, row: RowNumber, version: CommitVersion, state: Option<StoredRow>) {
		let history = self.rows.entry(row).or_default();
		match history.last_mut() {
			Some(last) if last.version == version => last.row = state,
			_ => history.push(Write {
				version,
				row: state,
			}),
		}
	}

	fn visible_after(&self, after: Option<RowNumber>, version: CommitVersion, limit: u64) -> Vec<Visible<'_>> {
		let lower = match after {
			Some(row) => Excluded(row),
			None => Unbounded,
		};
		let mut out = Vec::new();
		for (row_number, history) in self.rows.range((lower, Unbounded)) {
			if out.len() as u64 == limit {
				break;
			}
			let Some(write) = history.iter().rev().find(|write| write.version <= version) else {
				continue;
			};
			if let Some(row) = &write.row {
				out.push(Visible {
					row_number: *row_number,
					version: write.version,
					row,
				});
			}
		}
		out
	}

	fn chunk(&self, source: ObjectId, rows: &[Visible<'_>]) -> Result<RecordBatch> {
		let mut columns = Vec::with_capacity(self.columns.len());
		for (index, (name, ty)) in self.columns.iter().enumerate() {
			let mut builder = ColumnBuilder::with_capacity(ty.clone(), rows.len());
			for visible in rows {
				builder.push_value(visible.row.values[index].clone());
			}
			columns.push(builder.finish(name));
		}
		let mut out = batch(columns)?;
		out = with_system_column(
			out,
			SystemColumn::RowNumbers,
			uint8(SystemColumn::RowNumbers.name(), rows.iter().map(|visible| visible.row_number.0)).1,
		)?;
		out = with_system_column(
			out,
			SystemColumn::CreatedAt,
			datetime(SystemColumn::CreatedAt.name(), rows.iter().map(|visible| visible.row.created_at)).1,
		)?;
		out = with_system_column(
			out,
			SystemColumn::UpdatedAt,
			datetime(SystemColumn::UpdatedAt.name(), rows.iter().map(|visible| visible.row.updated_at)).1,
		)?;
		if self.time != TimeSource::None {
			out = with_system_column(
				out,
				SystemColumn::Time,
				datetime(SystemColumn::Time.name(), rows.iter().filter_map(|visible| visible.row.time))
					.1,
			)?;
		}
		if matches!(source, ObjectId::Table(_)) {
			out = with_system_column(
				out,
				SystemColumn::CommitVersion,
				uint8(SystemColumn::CommitVersion.name(), rows.iter().map(|visible| visible.version.0))
					.1,
			)?;
		}
		Ok(out)
	}
}

pub struct MemoryScan {
	sources: MemorySources,
	version: CommitVersion,
	cursor: Option<Cursor>,
}

struct Cursor {
	source: ObjectId,
	batch_size: NonZeroU64,
	after: Option<RowNumber>,
	pulled: bool,
	exhausted: bool,
}

impl Scan for MemoryScan {
	fn version(&self) -> CommitVersion {
		self.version
	}

	fn open(&mut self, source: ObjectId, batch_size: NonZeroU64) -> Result<()> {
		assert!(
			self.sources.0.lock().sources.contains_key(&source),
			"source {source:?} is not defined in the memory sources"
		);
		self.cursor = Some(Cursor {
			source,
			batch_size,
			after: None,
			pulled: false,
			exhausted: false,
		});
		Ok(())
	}

	fn next(&mut self) -> Result<Option<RecordBatch>> {
		let Some(cursor) = self.cursor.as_mut() else {
			panic!("memory scan pulled before any source was opened");
		};
		if cursor.exhausted {
			return Ok(None);
		}
		let store = self.sources.0.lock();
		let source = store.source(cursor.source);
		let limit = cursor.batch_size.get();
		let rows = source.visible_after(cursor.after, self.version, limit);
		let first = !cursor.pulled;
		cursor.pulled = true;
		if (rows.len() as u64) < limit {
			cursor.exhausted = true;
		}
		let Some(last) = rows.last() else {
			return match first {
				true => source.chunk(cursor.source, &rows).map(Some),
				false => Ok(None),
			};
		};
		cursor.after = Some(last.row_number);
		source.chunk(cursor.source, &rows).map(Some)
	}
}
