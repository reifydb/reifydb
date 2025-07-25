// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::delta::Delta;
use crate::row::EncodedRow;
use crate::{CowVec, EncodedKey, EncodedKeyRange, Error, Version};

#[derive(Debug)]
pub struct Versioned {
    pub key: EncodedKey,
    pub row: EncodedRow,
    pub version: Version,
}

#[derive(Debug)]
pub struct Unversioned {
    pub key: EncodedKey,
    pub row: EncodedRow,
}

pub trait VersionedStorage:
    Send
    + Sync
    + Clone
    + VersionedApply
    + VersionedGet
    + VersionedContains
    + VersionedScan
    + VersionedScanRev
    + VersionedScanRange
    + VersionedScanRangeRev
    + 'static
{
}

pub trait VersionedApply {
    fn apply(&self, delta: CowVec<Delta>, version: Version);
}

pub trait VersionedGet {
    fn get(&self, key: &EncodedKey, version: Version) -> Option<Versioned>;
}

pub trait VersionedContains {
    fn contains(&self, key: &EncodedKey, version: Version) -> bool;
}

pub trait VersionedIter: Iterator<Item = Versioned> + Send {}
impl<T: Send> VersionedIter for T where T: Iterator<Item = Versioned> {}

pub trait VersionedScan {
    type ScanIter<'a>: VersionedIter
    where
        Self: 'a;

    fn scan(&self, version: Version) -> Self::ScanIter<'_>;
}

pub trait VersionedScanRev {
    type ScanIterRev<'a>: VersionedIter
    where
        Self: 'a;

    fn scan_rev(&self, version: Version) -> Self::ScanIterRev<'_>;
}

pub trait VersionedScanRange {
    type ScanRangeIter<'a>: VersionedIter
    where
        Self: 'a;

    fn scan_range(&self, range: EncodedKeyRange, version: Version) -> Self::ScanRangeIter<'_>;

    fn scan_prefix(&self, prefix: &EncodedKey, version: Version) -> Self::ScanRangeIter<'_> {
        self.scan_range(EncodedKeyRange::prefix(prefix), version)
    }
}

pub trait VersionedScanRangeRev {
    type ScanRangeIterRev<'a>: VersionedIter
    where
        Self: 'a;

    fn scan_range_rev(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> Self::ScanRangeIterRev<'_>;

    fn scan_prefix_rev(&self, prefix: &EncodedKey, version: Version) -> Self::ScanRangeIterRev<'_> {
        self.scan_range_rev(EncodedKeyRange::prefix(prefix), version)
    }
}

pub trait UnversionedStorage:
    Send
    + Sync
    + Clone
    + UnversionedApply
    + UnversionedGet
    + UnversionedContains
    + UnversionedUpsert
    + UnversionedRemove
    + UnversionedScan
    + UnversionedScanRev
    + UnversionedScanRange
    + UnversionedScanRangeRev
    + 'static
{
}

pub trait UnversionedApply {
    fn apply(&mut self, delta: CowVec<Delta>) -> Result<(), Error>;
}

pub trait UnversionedGet {
    fn get(&self, key: &EncodedKey) -> Result<Option<Unversioned>, Error>;
}

pub trait UnversionedContains {
    fn contains(&self, key: &EncodedKey) -> Result<bool, Error>;
}

pub trait UnversionedUpsert: UnversionedApply {
    fn upsert(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), Error> {
        Self::apply(self, CowVec::new(vec![Delta::Upsert { key: key.clone(), row: row.clone() }]))
    }
}

pub trait UnversionedRemove: UnversionedApply {
    fn remove(&mut self, key: &EncodedKey) -> Result<(), Error> {
        Self::apply(self, CowVec::new(vec![Delta::Remove { key: key.clone() }]))
    }
}

pub trait UnversionedIter: Iterator<Item = Unversioned> {}
impl<T> UnversionedIter for T where T: Iterator<Item = Unversioned> {}

pub trait UnversionedScan {
    type ScanIter<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan(&self) -> Result<Self::ScanIter<'_>, Error>;
}

pub trait UnversionedScanRev {
    type ScanIterRev<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan_rev(&self) -> Result<Self::ScanIterRev<'_>, Error>;
}

pub trait UnversionedScanRange {
    type ScanRange<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan_range(&self, range: EncodedKeyRange) -> Result<Self::ScanRange<'_>, Error>;

    fn scan_prefix(&self, prefix: &EncodedKey) -> Result<Self::ScanRange<'_>, Error> {
        self.scan_range(EncodedKeyRange::prefix(prefix))
    }
}

pub trait UnversionedScanRangeRev {
    type ScanRangeRev<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan_range_rev(&self, range: EncodedKeyRange) -> Result<Self::ScanRangeRev<'_>, Error>;

    fn scan_prefix_rev(&self, prefix: &EncodedKey) -> Result<Self::ScanRangeRev<'_>, Error> {
        self.scan_range_rev(EncodedKeyRange::prefix(prefix))
    }
}
