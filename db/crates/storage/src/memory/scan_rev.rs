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
	interface::{
		Unversioned, UnversionedScanRev, Versioned, VersionedScanRev,
	},
	row::EncodedRow,
};

use crate::memory::{Memory, VersionedRow};

impl VersionedScanRev for Memory {
	type ScanIterRev<'a> = IterRev<'a>;

	fn scan_rev(
		&self,
		version: CommitVersion,
	) -> Result<Self::ScanIterRev<'_>> {
		let iter = self.versioned.iter();
		Ok(IterRev {
			iter: iter.rev(),
			version,
		})
	}
}

pub struct IterRev<'a> {
	pub(crate) iter: Rev<MapIter<'a, EncodedKey, VersionedRow>>,
	pub(crate) version: CommitVersion,
}

impl Iterator for IterRev<'_> {
	type Item = Versioned;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let item = self.iter.next()?;
			if let Some(row) = item.value().get(self.version) {
				return Some(Versioned {
					key: item.key().clone(),
					row,
					version: self.version,
				});
			}
		}
	}
}

impl UnversionedScanRev for Memory {
	type ScanIterRev<'a> = UnversionedIterRev<'a>;

	fn scan_rev(&self) -> Result<Self::ScanIterRev<'_>> {
		let iter = self.unversioned.iter();
		Ok(UnversionedIterRev {
			iter,
		})
	}
}

pub struct UnversionedIterRev<'a> {
	pub(crate) iter: MapIter<'a, EncodedKey, EncodedRow>,
}

impl Iterator for UnversionedIterRev<'_> {
	type Item = Unversioned;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.iter.next_back()?;
		Some(Unversioned {
			key: item.key().clone(),
			row: item.value().clone(),
		})
	}
}
