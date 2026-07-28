// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{
	util::{bitvec::BitVec, cowvec::CowVec},
	value::{datetime::DateTime, partition::Partition, row_number::RowNumber},
};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SystemColumns {
	row_numbers: CowVec<RowNumber>,
	partitions: CowVec<Partition>,
	created_at: CowVec<DateTime>,
	updated_at: CowVec<DateTime>,
	time: CowVec<DateTime>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RowStamps {
	pub row_number: Option<RowNumber>,
	pub partition: Option<Partition>,
	pub created_at: DateTime,
	pub updated_at: DateTime,
	pub time: DateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemColumn {
	RowNumbers,
	Partitions,
	CreatedAt,
	UpdatedAt,
	Time,
}

impl SystemColumn {
	pub const fn name(self) -> &'static str {
		match self {
			SystemColumn::RowNumbers => "#rownum",
			SystemColumn::Partitions => "#partition",
			SystemColumn::CreatedAt => "#created_at",
			SystemColumn::UpdatedAt => "#updated_at",
			SystemColumn::Time => "#time",
		}
	}
}

impl Display for SystemColumn {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.write_str(self.name())
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SystemColumnsError {
	#[error("cannot append rows: {column} is present on one side but not the other")]
	PresenceMismatch {
		column: SystemColumn,
		target_present: bool,
		source_present: bool,
	},

	#[error("{column} holds {len} entries but the batch has {row_count} rows")]
	LengthMismatch {
		column: SystemColumn,
		len: usize,
		row_count: usize,
	},
}

#[inline]
fn gather<T: Copy + PartialEq>(src: &CowVec<T>, indices: &[usize]) -> CowVec<T> {
	if src.is_empty() {
		return CowVec::default();
	}
	CowVec::new(indices.iter().map(|&i| src[i]).collect())
}

#[inline]
fn retain<T: Copy + PartialEq>(src: &CowVec<T>, mask: &BitVec) -> CowVec<T> {
	if src.is_empty() {
		return CowVec::default();
	}
	CowVec::new(src.iter().enumerate().filter(|(i, _)| *i < mask.len() && mask.get(*i)).map(|(_, &v)| v).collect())
}

#[inline]
fn head<T: Copy + PartialEq>(src: &CowVec<T>, n: usize) -> CowVec<T> {
	if src.is_empty() {
		return CowVec::default();
	}
	src.take(n)
}

#[inline]
fn concat<T: Copy + PartialEq>(dst: &mut CowVec<T>, src: &CowVec<T>) {
	if src.is_empty() {
		return;
	}
	dst.extend_from_slice(src.as_slice());
}

impl SystemColumns {
	pub fn empty() -> Self {
		Self::default()
	}

	pub fn new(
		row_numbers: Vec<RowNumber>,
		partitions: Vec<Partition>,
		created_at: Vec<DateTime>,
		updated_at: Vec<DateTime>,
		time: Vec<DateTime>,
	) -> Self {
		Self {
			row_numbers: CowVec::new(row_numbers),
			partitions: CowVec::new(partitions),
			created_at: CowVec::new(created_at),
			updated_at: CowVec::new(updated_at),
			time: CowVec::new(time),
		}
	}

	pub fn from_row_numbers(row_numbers: Vec<RowNumber>) -> Self {
		let n = row_numbers.len();
		let now = DateTime::default();
		Self::new(row_numbers, Vec::new(), vec![now; n], vec![now; n], vec![now; n])
	}

	pub fn set_row_numbers(&mut self, row_numbers: Vec<RowNumber>) {
		self.row_numbers = CowVec::new(row_numbers);
	}

	pub fn set_partitions(&mut self, partitions: Vec<Partition>) {
		self.partitions = CowVec::new(partitions);
	}

	pub fn set_created_at(&mut self, created_at: Vec<DateTime>) {
		self.created_at = CowVec::new(created_at);
	}

	pub fn set_updated_at(&mut self, updated_at: Vec<DateTime>) {
		self.updated_at = CowVec::new(updated_at);
	}

	pub fn set_time(&mut self, time: Vec<DateTime>) {
		self.time = CowVec::new(time);
	}
}

impl SystemColumns {
	#[inline]
	pub fn row_numbers(&self) -> &[RowNumber] {
		self.row_numbers.as_slice()
	}

	#[inline]
	pub fn partitions(&self) -> &[Partition] {
		self.partitions.as_slice()
	}

	#[inline]
	pub fn created_at(&self) -> &[DateTime] {
		self.created_at.as_slice()
	}

	#[inline]
	pub fn updated_at(&self) -> &[DateTime] {
		self.updated_at.as_slice()
	}

	#[inline]
	pub fn time(&self) -> &[DateTime] {
		self.time.as_slice()
	}

	pub fn row_count(&self) -> Option<usize> {
		let Self {
			row_numbers,
			partitions,
			created_at,
			updated_at,
			time,
		} = self;
		[row_numbers.len(), partitions.len(), created_at.len(), updated_at.len(), time.len()]
			.into_iter()
			.find(|&len| len > 0)
	}

	pub fn is_empty(&self) -> bool {
		self.row_count().is_none()
	}

	pub fn heap_size(&self) -> usize {
		let Self {
			row_numbers,
			partitions,
			created_at,
			updated_at,
			time,
		} = self;
		row_numbers.len() * size_of::<RowNumber>()
			+ partitions.len() * size_of::<Partition>()
			+ created_at.len() * size_of::<DateTime>()
			+ updated_at.len() * size_of::<DateTime>()
			+ time.len() * size_of::<DateTime>()
	}
}

impl SystemColumns {
	pub fn permute(&self, indices: &[usize]) -> Self {
		let Self {
			row_numbers,
			partitions,
			created_at,
			updated_at,
			time,
		} = self;
		Self {
			row_numbers: gather(row_numbers, indices),
			partitions: gather(partitions, indices),
			created_at: gather(created_at, indices),
			updated_at: gather(updated_at, indices),
			time: gather(time, indices),
		}
	}

	pub fn permute_in_place(&mut self, indices: &[usize]) {
		*self = self.permute(indices);
	}

	pub fn filter(&mut self, mask: &BitVec) {
		let Self {
			row_numbers,
			partitions,
			created_at,
			updated_at,
			time,
		} = self;
		*row_numbers = retain(row_numbers, mask);
		*partitions = retain(partitions, mask);
		*created_at = retain(created_at, mask);
		*updated_at = retain(updated_at, mask);
		*time = retain(time, mask);
	}

	pub fn take(&mut self, n: usize) {
		let Self {
			row_numbers,
			partitions,
			created_at,
			updated_at,
			time,
		} = self;
		*row_numbers = head(row_numbers, n);
		*partitions = head(partitions, n);
		*created_at = head(created_at, n);
		*updated_at = head(updated_at, n);
		*time = head(time, n);
	}

	pub fn extend(&mut self, source: &Self) -> Result<(), SystemColumnsError> {
		self.check_extendable(source)?;
		let Self {
			row_numbers,
			partitions,
			created_at,
			updated_at,
			time,
		} = self;
		concat(row_numbers, &source.row_numbers);
		concat(partitions, &source.partitions);
		concat(created_at, &source.created_at);
		concat(updated_at, &source.updated_at);
		concat(time, &source.time);
		Ok(())
	}

	pub fn append_indices(&mut self, source: &Self, indices: &[usize]) {
		let gathered = source.permute(indices);
		let Self {
			row_numbers,
			partitions,
			created_at,
			updated_at,
			time,
		} = self;
		concat(row_numbers, &gathered.row_numbers);
		concat(partitions, &gathered.partitions);
		concat(created_at, &gathered.created_at);
		concat(updated_at, &gathered.updated_at);
		concat(time, &gathered.time);
	}

	pub fn push(&mut self, stamps: RowStamps) {
		let RowStamps {
			row_number,
			partition,
			created_at,
			updated_at,
			time,
		} = stamps;
		if let Some(row_number) = row_number {
			self.row_numbers.push(row_number);
		}
		if let Some(partition) = partition {
			self.partitions.push(partition);
		}
		self.created_at.push(created_at);
		self.updated_at.push(updated_at);
		self.time.push(time);
	}

	pub fn clear(&mut self) {
		let Self {
			row_numbers,
			partitions,
			created_at,
			updated_at,
			time,
		} = self;
		row_numbers.clear();
		partitions.clear();
		created_at.clear();
		updated_at.clear();
		time.clear();
	}

	fn check_extendable(&self, source: &Self) -> Result<(), SystemColumnsError> {
		let Self {
			row_numbers,
			partitions,
			created_at,
			updated_at,
			time,
		} = self;
		let pairs = [
			(SystemColumn::RowNumbers, !row_numbers.is_empty(), !source.row_numbers.is_empty()),
			(SystemColumn::Partitions, !partitions.is_empty(), !source.partitions.is_empty()),
			(SystemColumn::CreatedAt, !created_at.is_empty(), !source.created_at.is_empty()),
			(SystemColumn::UpdatedAt, !updated_at.is_empty(), !source.updated_at.is_empty()),
			(SystemColumn::Time, !time.is_empty(), !source.time.is_empty()),
		];
		for (column, target_present, source_present) in pairs {
			if target_present != source_present {
				return Err(SystemColumnsError::PresenceMismatch {
					column,
					target_present,
					source_present,
				});
			}
		}
		Ok(())
	}

	pub fn validate(&self, row_count: usize) -> Result<(), SystemColumnsError> {
		let Self {
			row_numbers,
			partitions,
			created_at,
			updated_at,
			time,
		} = self;
		let lengths = [
			(SystemColumn::RowNumbers, row_numbers.len()),
			(SystemColumn::Partitions, partitions.len()),
			(SystemColumn::CreatedAt, created_at.len()),
			(SystemColumn::UpdatedAt, updated_at.len()),
			(SystemColumn::Time, time.len()),
		];
		for (column, len) in lengths {
			if len != 0 && len != row_count {
				return Err(SystemColumnsError::LengthMismatch {
					column,
					len,
					row_count,
				});
			}
		}
		Ok(())
	}

	#[track_caller]
	pub fn assert_invariants(&self, row_count: usize, ctx: &str) {
		if let Err(err) = self.validate(row_count) {
			panic!("{ctx}: {err}");
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn dt(n: u64) -> DateTime {
		DateTime::from_nanos(n)
	}

	fn partition(n: u128) -> Partition {
		Partition::from(n)
	}

	fn populated() -> SystemColumns {
		SystemColumns::new(
			(1..5).map(RowNumber::from).collect(),
			(0..4).map(|i| partition(i as u128)).collect(),
			(0..4).map(|i| dt(1000 + i)).collect(),
			(0..4).map(|i| dt(2000 + i)).collect(),
			(0..4).map(|i| dt(3000 + i)).collect(),
		)
	}

	#[track_caller]
	fn assert_row_matches(actual: &SystemColumns, at: usize, source: &SystemColumns, from: usize) {
		assert_eq!(actual.row_numbers()[at], source.row_numbers()[from], "row_numbers[{at}]");
		assert_eq!(actual.partitions()[at], source.partitions()[from], "partitions[{at}]");
		assert_eq!(actual.created_at()[at], source.created_at()[from], "created_at[{at}]");
		assert_eq!(actual.updated_at()[at], source.updated_at()[from], "updated_at[{at}]");
		assert_eq!(actual.time()[at], source.time()[from], "time[{at}]");
	}

	#[test]
	fn permute_moves_every_sidecar_with_its_row() {
		let source = populated();
		let indices = [3, 0, 2, 1];
		let permuted = source.permute(&indices);

		assert_eq!(permuted.row_count(), Some(4));
		for (at, &from) in indices.iter().enumerate() {
			assert_row_matches(&permuted, at, &source, from);
		}
	}

	#[test]
	fn permute_trims_when_given_fewer_indices_than_rows() {
		let source = populated();
		let indices = [2, 0];
		let permuted = source.permute(&indices);

		assert_eq!(permuted.row_count(), Some(2));
		for (at, &from) in indices.iter().enumerate() {
			assert_row_matches(&permuted, at, &source, from);
		}
	}

	#[test]
	fn permute_duplicates_a_repeated_index() {
		let source = populated();
		let permuted = source.permute(&[1, 1, 1]);

		assert_eq!(permuted.row_count(), Some(3));
		for at in 0..3 {
			assert_row_matches(&permuted, at, &source, 1);
		}
	}

	#[test]
	fn filter_keeps_masked_rows_intact() {
		let source = populated();
		let mut filtered = source.clone();
		filtered.filter(&BitVec::from_slice(&[false, true, false, true]));

		assert_eq!(filtered.row_count(), Some(2));
		assert_row_matches(&filtered, 0, &source, 1);
		assert_row_matches(&filtered, 1, &source, 3);
	}

	#[test]
	fn take_trims_every_sidecar() {
		let source = populated();
		let mut taken = source.clone();
		taken.take(2);

		assert_eq!(taken.row_count(), Some(2));
		assert_row_matches(&taken, 0, &source, 0);
		assert_row_matches(&taken, 1, &source, 1);
	}

	#[test]
	fn take_beyond_the_row_count_is_a_noop() {
		let source = populated();
		let mut taken = source.clone();
		taken.take(99);
		assert_eq!(taken, source);
	}

	#[test]
	fn extend_concatenates_every_sidecar() {
		let source = populated();
		let mut acc = source.clone();
		acc.extend(&source).unwrap();

		assert_eq!(acc.row_count(), Some(8));
		for i in 0..4 {
			assert_row_matches(&acc, i, &source, i);
			assert_row_matches(&acc, i + 4, &source, i);
		}
	}

	#[test]
	fn extend_rejects_a_presence_mismatch() {
		let mut acc = populated();
		let mut source = populated();
		source.set_partitions(Vec::new());

		assert_eq!(
			acc.extend(&source).unwrap_err(),
			SystemColumnsError::PresenceMismatch {
				column: SystemColumn::Partitions,
				target_present: true,
				source_present: false,
			}
		);
	}

	#[test]
	fn append_indices_appends_only_the_named_rows() {
		let source = populated();
		let mut acc = source.clone();
		acc.append_indices(&source, &[3, 1]);

		assert_eq!(acc.row_count(), Some(6));
		assert_row_matches(&acc, 4, &source, 3);
		assert_row_matches(&acc, 5, &source, 1);
	}

	#[test]
	fn every_operation_leaves_an_absent_sidecar_absent() {
		let mut source = populated();
		source.set_partitions(Vec::new());

		assert!(source.permute(&[1, 0]).partitions().is_empty(), "permute");

		let mut filtered = source.clone();
		filtered.filter(&BitVec::from_slice(&[true, false, true, false]));
		assert!(filtered.partitions().is_empty(), "filter");

		let mut taken = source.clone();
		taken.take(2);
		assert!(taken.partitions().is_empty(), "take");

		let mut extended = source.clone();
		extended.extend(&source).unwrap();
		assert!(extended.partitions().is_empty(), "extend");

		let mut appended = source.clone();
		appended.append_indices(&source, &[0]);
		assert!(appended.partitions().is_empty(), "append_indices");
	}

	#[test]
	fn permuting_by_the_inverse_restores_the_original() {
		let source = populated();
		let forward = [2, 3, 1, 0];
		let mut inverse = [0usize; 4];
		for (at, &from) in forward.iter().enumerate() {
			inverse[from] = at;
		}
		assert_eq!(source.permute(&forward).permute(&inverse), source);
	}

	#[test]
	fn push_appends_one_row_to_every_sidecar() {
		let mut acc = SystemColumns::empty();
		acc.push(RowStamps {
			row_number: Some(RowNumber::from(7)),
			partition: Some(partition(2)),
			created_at: dt(10),
			updated_at: dt(20),
			time: dt(30),
		});

		assert_eq!(acc.row_count(), Some(1));
		assert_eq!(acc.row_numbers(), &[RowNumber::from(7)]);
		assert_eq!(acc.partitions(), &[partition(2)]);
		assert_eq!(acc.created_at(), &[dt(10)]);
		assert_eq!(acc.updated_at(), &[dt(20)]);
		assert_eq!(acc.time(), &[dt(30)]);
	}

	#[test]
	fn clear_empties_every_sidecar() {
		let mut acc = populated();
		acc.clear();
		assert_eq!(acc.row_count(), None);
		assert!(acc.is_empty());
	}

	#[test]
	#[should_panic(expected = "time")]
	fn assert_invariants_rejects_a_partial_sidecar() {
		let mut partial = populated();
		partial.time = CowVec::new(vec![dt(1)]);
		partial.assert_invariants(4, "test");
	}
}
