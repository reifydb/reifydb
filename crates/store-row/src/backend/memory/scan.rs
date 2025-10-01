// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crossbeam_skiplist::map::Iter as MapIter;
use reifydb_core::{
	CommitVersion, EncodedKey, Result,
	interface::{MultiVersionRow, MultiVersionScan, SingleVersionRow, SingleVersionScan},
	value::row::EncodedRow,
};

use crate::backend::memory::{Memory, MultiVersionRowContainer};

impl MultiVersionScan for Memory {
	type ScanIter<'a> = MultiVersionIter<'a>;

	fn scan(&self, version: CommitVersion) -> Result<Self::ScanIter<'_>> {
		let iter = self.multi.iter();
		Ok(MultiVersionIter {
			iter,
			version,
		})
	}
}

pub struct MultiVersionIter<'a> {
	pub(crate) iter: MapIter<'a, EncodedKey, MultiVersionRowContainer>,
	pub(crate) version: CommitVersion,
}

impl Iterator for MultiVersionIter<'_> {
	type Item = MultiVersionRow;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let item = self.iter.next()?;
			if let Some(row) = item.value().get(self.version) {
				return Some(MultiVersionRow {
					key: item.key().clone(),
					row,
					version: self.version,
				});
			}
		}
	}
}

impl SingleVersionScan for Memory {
	type ScanIter<'a> = SingleVersionIter<'a>;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		let iter = self.single.iter();
		Ok(SingleVersionIter {
			iter,
		})
	}
}

pub struct SingleVersionIter<'a> {
	pub(crate) iter: MapIter<'a, EncodedKey, EncodedRow>,
}

impl Iterator for SingleVersionIter<'_> {
	type Item = SingleVersionRow;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.iter.next()?;
		Some(SingleVersionRow {
			key: item.key().clone(),
			row: item.value().clone(),
		})
	}
}
