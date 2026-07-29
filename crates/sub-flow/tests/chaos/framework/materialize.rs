// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	interface::change::{Change, Diff},
	value::column::columns::Columns,
};
use reifydb_value::value::{Value, row_number::RowNumber};

#[derive(Default)]
pub struct View {
	rows: BTreeMap<RowNumber, Vec<Value>>,
	pub incoherent: Vec<String>,
}

impl View {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn apply(&mut self, change: &Change) {
		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					for (number, values) in rows_of(post) {
						if self.rows.insert(number, values).is_some() {
							self.incoherent.push(format!(
								"insert of row {number:?} that already existed"
							));
						}
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					for (number, _) in rows_of(pre) {
						if !self.rows.contains_key(&number) {
							self.incoherent.push(format!(
								"update whose pre row {number:?} is absent"
							));
						}
					}
					for (number, values) in rows_of(post) {
						self.rows.insert(number, values);
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					for (number, _) in rows_of(pre) {
						if self.rows.remove(&number).is_none() {
							self.incoherent
								.push(format!("remove of absent row {number:?}"));
						}
					}
				}
			}
		}
	}

	pub fn len(&self) -> usize {
		self.rows.len()
	}

	pub fn is_empty(&self) -> bool {
		self.rows.is_empty()
	}

	pub fn projected(&self, indices: &[usize]) -> Vec<Vec<Value>> {
		let mut out: Vec<Vec<Value>> =
			self.rows.values().map(|values| indices.iter().map(|i| values[*i].clone()).collect()).collect();
		out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
		out
	}

	pub fn rows(&self) -> impl Iterator<Item = (&RowNumber, &Vec<Value>)> {
		self.rows.iter()
	}
}

fn rows_of(columns: &Columns) -> Vec<(RowNumber, Vec<Value>)> {
	(0..columns.row_count()).map(|i| (columns.row_numbers()[i], columns.row(i))).collect()
}
