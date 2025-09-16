// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::collections::VecDeque;

use reifydb_core::{
	CommitVersion, EncodedKey, Result,
	interface::{Versioned, VersionedScan},
};

use super::{execute_scan_query, get_table_names};
use crate::sqlite::{Sqlite, read::Reader};

impl VersionedScan for Sqlite {
	type ScanIter<'a> = Iter;

	fn scan(&self, version: CommitVersion) -> Result<Self::ScanIter<'_>> {
		Ok(Iter::new(self.get_reader(), version, 1024))
	}
}

pub struct Iter {
	conn: Reader,
	version: CommitVersion,
	table_names: Vec<String>,
	buffer: VecDeque<Versioned>,
	last_key: Option<EncodedKey>,
	batch_size: usize,
	exhausted: bool,
}

impl Iter {
	pub fn new(conn: Reader, version: CommitVersion, batch_size: usize) -> Self {
		let table_names = get_table_names(&conn);

		Self {
			conn,
			version,
			table_names,
			buffer: VecDeque::new(),
			last_key: None,
			batch_size,
			exhausted: false,
		}
	}

	fn refill_buffer(&mut self) {
		if self.exhausted {
			return;
		}

		self.buffer.clear();

		let count = execute_scan_query(
			&self.conn,
			&self.table_names,
			self.version,
			self.batch_size,
			self.last_key.as_ref(),
			"ASC",
			&mut self.buffer,
		);

		// Update last_key to the last item we retrieved
		if let Some(last_item) = self.buffer.back() {
			self.last_key = Some(last_item.key.clone());
		}

		// If we got fewer results than requested, we've reached the end
		if count < self.batch_size {
			self.exhausted = true;
		}
	}
}

impl Iterator for Iter {
	type Item = Versioned;

	fn next(&mut self) -> Option<Self::Item> {
		if self.buffer.is_empty() {
			self.refill_buffer();
		}
		self.buffer.pop_front()
	}
}
