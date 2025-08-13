// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Result, Version,
	interface::{
		Unversioned, UnversionedScanRev, Versioned, VersionedScanRev,
	},
};

use crate::lmdb::Lmdb;

impl VersionedScanRev for Lmdb {
	type ScanIterRev<'a> = IterRev;

	fn scan_rev(&self, _version: Version) -> Result<Self::ScanIterRev<'_>> {
		todo!()
	}
}

pub struct IterRev {}

impl Iterator for IterRev {
	type Item = Versioned;

	fn next(&mut self) -> Option<Self::Item> {
		todo!()
	}
}

pub struct UnversionedIterRev {}

impl<'a> Iterator for UnversionedIterRev {
	type Item = Unversioned;

	fn next(&mut self) -> Option<Self::Item> {
		todo!()
	}
}

impl UnversionedScanRev for Lmdb {
	type ScanIterRev<'a> = UnversionedIterRev;

	fn scan_rev(&self) -> Result<Self::ScanIterRev<'_>> {
		todo!()
	}
}
