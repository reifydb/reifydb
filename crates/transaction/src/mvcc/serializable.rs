// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::mvcc::transaction::serializable::{ReadTransaction, Serializable, WriteTransaction};
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    BoxedVersionedIter, GetHooks, UnversionedTransaction, Versioned, VersionedReadTransaction,
    VersionedStorage, VersionedTransaction, VersionedWriteTransaction,
};
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange, Error};

impl<VS: VersionedStorage, UT: UnversionedTransaction> GetHooks for Serializable<VS, UT> {
    fn get_hooks(&self) -> &Hooks {
        &self.hooks
    }
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> VersionedTransaction
    for Serializable<VS, UT>
{
    type Read = ReadTransaction<VS, UT>;
    type Write = WriteTransaction<VS, UT>;

    fn begin_read(&self) -> Result<Self::Read, Error> {
        self.begin_read()
    }

    fn begin_write(&self) -> Result<Self::Write, Error> {
        self.begin_write()
    }
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> VersionedReadTransaction
    for ReadTransaction<VS, UT>
{
    fn get(&mut self, key: &EncodedKey) -> Result<Option<Versioned>, Error> {
        Ok(ReadTransaction::get(self, key)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        }))
    }

    fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
        ReadTransaction::contains_key(self, key)
    }

    fn scan(&mut self) -> Result<BoxedVersionedIter, Error> {
        let iter = ReadTransaction::scan(self)?;
        Ok(Box::new(iter.into_iter()))
    }

    fn scan_rev(&mut self) -> Result<BoxedVersionedIter, Error> {
        let iter = ReadTransaction::scan_rev(self)?;
        Ok(Box::new(iter.into_iter()))
    }

    fn range(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error> {
        let iter = ReadTransaction::scan_range(self, range)?;
        Ok(Box::new(iter.into_iter()))
    }

    fn range_rev(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error> {
        let iter = ReadTransaction::scan_range_rev(self, range)?;
        Ok(Box::new(iter.into_iter()))
    }

    fn prefix(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error> {
        let iter = ReadTransaction::scan_prefix(self, prefix)?;
        Ok(Box::new(iter.into_iter()))
    }

    fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error> {
        let iter = ReadTransaction::scan_prefix_rev(self, prefix)?;
        Ok(Box::new(iter.into_iter()))
    }
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> VersionedReadTransaction
    for WriteTransaction<VS, UT>
{
    fn get(&mut self, key: &EncodedKey) -> Result<Option<Versioned>, Error> {
        Ok(WriteTransaction::get(self, key)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        }))
    }

    fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
        Ok(WriteTransaction::contains_key(self, key)?)
    }

    fn scan(&mut self) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan()?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_rev(&mut self) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan_rev()?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn range(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan_range(range)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn range_rev(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan_range_rev(range)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn prefix(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan_prefix(prefix)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan_prefix_rev(prefix)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> VersionedWriteTransaction
    for WriteTransaction<VS, UT>
{
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), Error> {
        WriteTransaction::set(self, key, row)?;
        Ok(())
    }

    fn remove(&mut self, key: &EncodedKey) -> Result<(), Error> {
        WriteTransaction::remove(self, key)?;
        Ok(())
    }

    fn commit(mut self) -> Result<(), Error> {
        WriteTransaction::commit(&mut self)?;
        Ok(())
    }

    fn rollback(mut self) -> Result<(), Error> {
        WriteTransaction::rollback(&mut self)?;
        Ok(())
    }
}
