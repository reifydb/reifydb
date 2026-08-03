// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::Value;

pub const DEFAULT_INSERT_BATCH: usize = 1000;

pub type RowGenerator = fn(index: u64, scale: u64) -> Vec<Value>;

pub enum Dataset {
	Manual(ManualDataset),
	Generated(GeneratedDataset),
}

pub struct ManualDataset {
	pub ddl: Vec<String>,
	pub rows: Vec<String>,
}

pub struct GeneratedDataset {
	pub ddl: Vec<String>,
	pub seeds: Vec<TableSeed>,
}

pub struct TableSeed {
	pub table: &'static str,
	pub columns: &'static [&'static str],
	pub count: RowCount,
	pub row: RowGenerator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowCount {
	Scaled,
	ScaledTimes(u64),
	Fixed(u64),
}

impl RowCount {
	pub fn resolve(&self, scale: u64) -> u64 {
		match self {
			RowCount::Scaled => scale,
			RowCount::ScaledTimes(factor) => scale.saturating_mul(*factor),
			RowCount::Fixed(count) => *count,
		}
	}
}

impl TableSeed {
	pub fn row_count(&self, scale: u64) -> u64 {
		self.count.resolve(scale)
	}

	pub fn rows(&self, scale: u64) -> impl Iterator<Item = Vec<Value>> + '_ {
		let generator = self.row;
		(0..self.row_count(scale)).map(move |index| generator(index, scale))
	}
}

impl Dataset {
	pub fn manual(ddl: Vec<String>, rows: Vec<String>) -> Self {
		Dataset::Manual(ManualDataset {
			ddl,
			rows,
		})
	}

	pub fn generated(ddl: Vec<String>, seeds: Vec<TableSeed>) -> Self {
		Dataset::Generated(GeneratedDataset {
			ddl,
			seeds,
		})
	}

	pub fn is_manual(&self) -> bool {
		matches!(self, Dataset::Manual(_))
	}

	pub fn ddl(&self) -> &[String] {
		match self {
			Dataset::Manual(dataset) => &dataset.ddl,
			Dataset::Generated(dataset) => &dataset.ddl,
		}
	}

	pub fn row_count(&self, scale: u64) -> u64 {
		match self {
			Dataset::Manual(dataset) => dataset.rows.len() as u64,
			Dataset::Generated(dataset) => dataset.seeds.iter().map(|seed| seed.row_count(scale)).sum(),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use crate::dataset::{Dataset, RowCount, TableSeed};

	fn seed(count: RowCount) -> TableSeed {
		TableSeed {
			table: "bench::t",
			columns: &["id", "peer"],
			count,
			row: |index, scale| vec![Value::Int8(index as i64), Value::Int8((index % scale.max(1)) as i64)],
		}
	}

	#[test]
	fn scaled_row_count_follows_the_profile_scale() {
		assert_eq!(seed(RowCount::Scaled).row_count(10_000), 10_000);
		assert_eq!(seed(RowCount::Scaled).row_count(1_000_000), 1_000_000);
	}

	#[test]
	fn scaled_times_multiplies_the_profile_scale() {
		// The join scenario needs orders to fan out against customers; if this collapsed to a
		// one-to-one count the join would stop measuring fan-out entirely.
		assert_eq!(seed(RowCount::ScaledTimes(3)).row_count(10_000), 30_000);
	}

	#[test]
	fn fixed_row_count_ignores_the_profile_scale() {
		assert_eq!(seed(RowCount::Fixed(7)).row_count(1_000_000), 7);
	}

	#[test]
	fn scaled_times_saturates_rather_than_overflowing() {
		assert_eq!(seed(RowCount::ScaledTimes(4)).row_count(u64::MAX), u64::MAX);
	}

	#[test]
	fn generated_rows_see_both_index_and_scale() {
		// The scale argument exists so a generator can derive cross-table references such as
		// `customer_id = index % customers`. Dropping it would silently break the join scenario.
		let seed = seed(RowCount::Fixed(3));
		let rows: Vec<Vec<Value>> = seed.rows(2).collect();

		assert_eq!(rows.len(), 3);
		assert_eq!(rows[2][0], Value::Int8(2));
		assert_eq!(rows[2][1], Value::Int8(0));
	}

	#[test]
	fn manual_dataset_row_count_is_its_literal_statement_count() {
		let dataset = Dataset::manual(
			vec!["create table bench::t { id: int8 }".to_string()],
			vec!["INSERT bench::t [{ id: 1 }]".to_string()],
		);

		assert_eq!(dataset.row_count(1_000_000), 1);
		assert!(dataset.is_manual());
	}
}
