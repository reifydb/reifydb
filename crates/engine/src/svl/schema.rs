// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::schema::{SchemaName, Store};
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
    fn get(&self, name: impl AsRef<str>) -> crate::Result<&Store> {
        let name = name.as_ref();
        Ok(self.stores.get(name).unwrap())
    }

    fn list(&self) -> crate::Result<Vec<&Store>> {
        todo!()
    }
}

impl crate::SchemaMut for Schema {
    fn create(&mut self, store: Store) -> crate::Result<()> {
        assert!(self.stores.get(store.name.deref()).is_none());
        self.stores.insert(store.name.deref().to_owned(), store);
        Ok(())
    }

    fn create_if_not_exists(&mut self, store: Store) -> crate::Result<()> {
        todo!()
    }

    fn drop(&mut self, name: impl AsRef<str>) -> crate::Result<()> {
        todo!()
    }
}
