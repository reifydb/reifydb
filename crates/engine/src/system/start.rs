// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::hook::lifecycle::{OnCreateHook, OnInitHook};
use reifydb_core::hook::{BoxedHookIter, Callback};
use reifydb_core::interface::{EncodableKey, SystemVersion, SystemVersionKey};
use reifydb_core::interface::{Transaction, UnversionedStorage, VersionedStorage};
use reifydb_core::row::Layout;
use reifydb_core::{Type, return_hooks};
use std::marker::PhantomData;

pub(crate) struct SystemStartCallback<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    transaction: T,
    _phantom: PhantomData<(VS, US)>,
}

impl<VS, US, T> SystemStartCallback<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub(crate) fn new(transaction: T) -> Self {
        Self { transaction, _phantom: PhantomData }
    }
}

const CURRENT_STORAGE_VERSION: u8 = 0x01;

impl<VS, US, T> Callback<OnInitHook> for SystemStartCallback<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn on(&self, _hook: &OnInitHook) -> Result<BoxedHookIter, reifydb_core::Error> {
        let layout = Layout::new(&[Type::Uint1]);
        let key = SystemVersionKey { version: SystemVersion::Storage }.encode();

        let mut unversioned = self.transaction.begin_unversioned_tx();
        match unversioned.get(&key)? {
            None => {
                let mut row = layout.allocate_row();
                layout.set_u8(&mut row, 0, CURRENT_STORAGE_VERSION);
                unversioned.upsert(&key, row)?;

                // the database was never started before
                self.trigger_database_creation()?
            }
            Some(unversioned) => {
                let version = layout.get_u8(&unversioned.row, 0);
                assert_eq!(CURRENT_STORAGE_VERSION, version, "Storage version mismatch");
            }
        }

        return_hooks!()
    }
}

impl<VS, US, T> SystemStartCallback<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn trigger_database_creation(&self) -> crate::Result<()> {
        self.transaction.get_hooks().trigger(OnCreateHook {})
    }
}
