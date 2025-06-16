// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use r#impl::DepStore;

mod r#impl;

use crate::DepColumn;
use reifydb_core::StoreKind;

pub trait DepStoreRx {
    fn kind(&self) -> crate::Result<StoreKind>;

    fn get_column(&self, column: &str) -> crate::Result<DepColumn>;

    fn list_columns(&self) -> crate::Result<Vec<DepColumn>>;

    fn get_column_index(&self, column: &str) -> crate::Result<usize>;
}

pub trait DepStoreTx: DepStoreRx {}

pub struct DepNopStore {}

impl DepStoreRx for DepNopStore {
    fn kind(&self) -> crate::Result<StoreKind> {
        todo!()
    }

    fn get_column(&self, _column: &str) -> crate::Result<DepColumn> {
        unreachable!()
    }

    fn list_columns(&self) -> crate::Result<Vec<DepColumn>> {
        unreachable!()
    }

    fn get_column_index(&self, _column: &str) -> crate::Result<usize> {
        unreachable!()
    }
}
