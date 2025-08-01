// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::delta::Delta;
use crate::row::EncodedRow;
use crate::{CowVec, EncodedKey, EncodedKeyRange, Version};

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
    fn apply(&self, delta: CowVec<Delta>, version: Version) -> crate::Result<()>;
}

pub trait VersionedGet {
    fn get(&self, key: &EncodedKey, version: Version) -> crate::Result<Option<Versioned>>;
}

pub trait VersionedContains {
    fn contains(&self, key: &EncodedKey, version: Version) -> crate::Result<bool>;
}

pub trait VersionedIter: Iterator<Item = Versioned> + Send {}
impl<T: Send> VersionedIter for T where T: Iterator<Item = Versioned> {}

pub trait VersionedScan {
    type ScanIter<'a>: VersionedIter
    where
        Self: 'a;

    fn scan(&self, version: Version) -> crate::Result<Self::ScanIter<'_>>;
}

pub trait VersionedScanRev {
    type ScanIterRev<'a>: VersionedIter
    where
        Self: 'a;

    fn scan_rev(&self, version: Version) -> crate::Result<Self::ScanIterRev<'_>>;
}

pub trait VersionedScanRange {
    type ScanRangeIter<'a>: VersionedIter
    where
        Self: 'a;

    fn scan_range(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> crate::Result<Self::ScanRangeIter<'_>>;

    fn scan_prefix(
        &self,
        prefix: &EncodedKey,
        version: Version,
    ) -> crate::Result<Self::ScanRangeIter<'_>> {
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
    ) -> crate::Result<Self::ScanRangeIterRev<'_>>;

    fn scan_prefix_rev(
        &self,
        prefix: &EncodedKey,
        version: Version,
    ) -> crate::Result<Self::ScanRangeIterRev<'_>> {
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
    fn apply(&mut self, delta: CowVec<Delta>) -> crate::Result<()>;
}

pub trait UnversionedGet {
    fn get(&self, key: &EncodedKey) -> crate::Result<Option<Unversioned>>;
}

pub trait UnversionedContains {
    fn contains(&self, key: &EncodedKey) -> crate::Result<bool>;
}

pub trait UnversionedUpsert: UnversionedApply {
    fn upsert(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()> {
        Self::apply(self, CowVec::new(vec![Delta::Upsert { key: key.clone(), row: row.clone() }]))
    }
}

pub trait UnversionedRemove: UnversionedApply {
    fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
        Self::apply(self, CowVec::new(vec![Delta::Remove { key: key.clone() }]))
    }
}

pub trait UnversionedIter: Iterator<Item = Unversioned> + Send {}
impl<T> UnversionedIter for T where T: Iterator<Item = Unversioned> + Send {}

pub trait UnversionedScan {
    type ScanIter<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan(&self) -> crate::Result<Self::ScanIter<'_>>;
}

pub trait UnversionedScanRev {
    type ScanIterRev<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan_rev(&self) -> crate::Result<Self::ScanIterRev<'_>>;
}

pub trait UnversionedScanRange {
    type ScanRange<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan_range(&self, range: EncodedKeyRange) -> crate::Result<Self::ScanRange<'_>>;

    fn scan_prefix(&self, prefix: &EncodedKey) -> crate::Result<Self::ScanRange<'_>> {
        self.scan_range(EncodedKeyRange::prefix(prefix))
    }
}

pub trait UnversionedScanRangeRev {
    type ScanRangeRev<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan_range_rev(&self, range: EncodedKeyRange) -> crate::Result<Self::ScanRangeRev<'_>>;

    fn scan_prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<Self::ScanRangeRev<'_>> {
        self.scan_range_rev(EncodedKeyRange::prefix(prefix))
    }
}
