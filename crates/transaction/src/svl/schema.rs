// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::svl::store::{ Store};
use base::{Column, StoreToCreate};
use base::schema::SchemaName;
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

impl base::Schema for Schema {
    type Store = Store;
    fn get(&self, name: impl AsRef<str>) -> base::Result<&Store> {
        let name = name.as_ref();
        Ok(self.stores.get(name).unwrap())
    }

    fn list(&self) -> base::Result<Vec<&Store>> {
        todo!()
    }
}

impl base::SchemaMut for Schema {
    type StoreMut = Store;

    fn create(&mut self, to_create: StoreToCreate) -> base::Result<()> {
        match to_create {
            StoreToCreate::Table { name, columns } => {
                assert!(self.stores.get(name.deref()).is_none());
                self.stores.insert(
                    name.deref().to_string(),
                    Store {
                        name,
                        columns: columns
                            .into_iter()
                            .map(|c| Column { name: c.name, value: c.value, default: c.default })
                            .collect::<Vec<_>>(),
                    },
                );
            }
        }
        Ok(())
    }

    fn create_if_not_exists(&mut self, to_create: StoreToCreate) -> base::Result<()> {
        todo!()
    }

    fn drop(&mut self, name: impl AsRef<str>) -> base::Result<()> {
        todo!()
    }
}
