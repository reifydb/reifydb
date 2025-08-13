// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Result, Version,
	interface::{Unversioned, UnversionedScan, Versioned, VersionedScan},
};

use crate::lmdb::Lmdb;

impl VersionedScan for Lmdb {
	type ScanIter<'a> = Iter;

	fn scan(&self, _version: Version) -> Result<Self::ScanIter<'_>> {
		todo!()
	}
}

pub struct Iter {}

impl Iterator for Iter {
	type Item = Versioned;

	fn next(&mut self) -> Option<Self::Item> {
		todo!()
	}
}

pub struct UnversionedIter {}

impl<'a> Iterator for UnversionedIter {
	type Item = Unversioned;

	fn next(&mut self) -> Option<Self::Item> {
		todo!()
	}
}

impl UnversionedScan for Lmdb {
	type ScanIter<'a> = UnversionedIter;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		todo!()
	}
}
