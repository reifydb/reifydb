// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::svl::store::Store;
use base::schema::{SchemaName, StoreKind};
use std::collections::HashMap;
use std::ops::Deref;

pub struct Schema {
    name: SchemaName,
    stores: HashMap<String, Store>,
}

impl Schema {
    pub fn new(name: SchemaName) -> Self {
        Self { name, stores: HashMap::new() }
    }
}

impl crate::Schema for Schema {
    type Store = Store;
    fn get(&self, name: impl AsRef<str>) -> crate::Result<&Store> {
        let name = name.as_ref();
        Ok(self.stores.get(name).unwrap())
    }

    fn list(&self) -> crate::Result<Vec<&Store>> {
        todo!()
    }
}

impl crate::SchemaMut for Schema {
    type StoreMut = Store;

    fn create(&mut self, store: base::schema::Store) -> crate::Result<()> {
        assert!(self.stores.get(store.name.deref()).is_none());

        let columns = match store.kind {
            StoreKind::Table(table) => table.columns,
        };

        self.stores.insert(store.name.deref().to_owned(), Store { name: store.name, columns });
        Ok(())
    }

    fn create_if_not_exists(&mut self, store: base::schema::Store) -> crate::Result<()> {
        todo!()
    }

    fn drop(&mut self, name: impl AsRef<str>) -> crate::Result<()> {
        todo!()
    }
}
