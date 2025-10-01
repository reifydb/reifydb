// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::iter::Rev;

use crossbeam_skiplist::map::Iter as MapIter;
use reifydb_core::{
	CommitVersion, EncodedKey, Result,
	interface::{MultiVersionRow, MultiVersionScanRev, SingleVersionRow, SingleVersionScanRev},
	value::row::EncodedRow,
};

use crate::backend::memory::{Memory, MultiVersionRowContainer};

impl MultiVersionScanRev for Memory {
	type ScanIterRev<'a> = IterRev<'a>;

	fn scan_rev(&self, version: CommitVersion) -> Result<Self::ScanIterRev<'_>> {
		let iter = self.multi.iter();
		Ok(IterRev {
			iter: iter.rev(),
			version,
		})
	}
}

pub struct IterRev<'a> {
	pub(crate) iter: Rev<MapIter<'a, EncodedKey, MultiVersionRowContainer>>,
	pub(crate) version: CommitVersion,
}

impl Iterator for IterRev<'_> {
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

impl SingleVersionScanRev for Memory {
	type ScanIterRev<'a> = SingleVersionIterRev<'a>;

	fn scan_rev(&self) -> Result<Self::ScanIterRev<'_>> {
		let iter = self.single.iter();
		Ok(SingleVersionIterRev {
			iter,
		})
	}
}

pub struct SingleVersionIterRev<'a> {
	pub(crate) iter: MapIter<'a, EncodedKey, EncodedRow>,
}

impl Iterator for SingleVersionIterRev<'_> {
	type Item = SingleVersionRow;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.iter.next_back()?;
		Some(SingleVersionRow {
			key: item.key().clone(),
			row: item.value().clone(),
		})
	}
}
