// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::hook::lifecycle::{OnCreateHook, OnInitHook};
use reifydb_core::hook::{BoxedHookIter, Callback};
use reifydb_core::interface::{
    EncodableKey, SystemVersion, SystemVersionKey, UnversionedReadTransaction,
    UnversionedTransaction, UnversionedWriteTransaction,
};
use reifydb_core::row::EncodedRowLayout;
use reifydb_core::{Type, return_hooks};

pub(crate) struct StartCallback<UT>
where
    UT: UnversionedTransaction,
{
    unversioned: UT,
}

impl<UT> StartCallback<UT>
where
    UT: UnversionedTransaction,
{
    pub(crate) fn new(unversioned: UT) -> Self {
        Self { unversioned }
    }
}

const CURRENT_STORAGE_VERSION: u8 = 0x01;

impl<UT> Callback<OnInitHook> for StartCallback<UT>
where
    UT: UnversionedTransaction,
{
    fn on(&self, _hook: &OnInitHook) -> crate::Result<BoxedHookIter> {
        let layout = EncodedRowLayout::new(&[Type::Uint1]);
        let key = SystemVersionKey { version: SystemVersion::Storage }.encode();

        let created = self.unversioned.with_write(|tx| match tx.get(&key)? {
            None => {
                let mut row = layout.allocate_row();
                layout.set_u8(&mut row, 0, CURRENT_STORAGE_VERSION);
                tx.set(&key, row)?;
                Ok(true)
            }
            Some(unversioned) => {
                let version = layout.get_u8(&unversioned.row, 0);
                assert_eq!(CURRENT_STORAGE_VERSION, version, "Storage version mismatch");
                Ok(false)
            }
        })?;

        // the database was never started before
        if created {
            self.trigger_database_creation()?
        }

        return_hooks!()
    }
}

impl<UT> StartCallback<UT>
where
    UT: UnversionedTransaction,
{
    fn trigger_database_creation(&self) -> crate::Result<()> {
        self.unversioned.get_hooks().trigger(OnCreateHook {})
    }
}
