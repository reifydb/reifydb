// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::StoreToCreate;
use crate::catalog::{Column, Store};
use reifydb_core::StoreKind;
use std::collections::HashMap;
use std::ops::Deref;

#[derive(Debug)]
pub struct Schema {
    name: String,
    stores: HashMap<String, Store>,
}

impl Schema {
    pub fn new(name: String) -> Self {
        Self { name, stores: HashMap::new() }
    }
}

impl crate::SchemaRx for Schema {
    type StoreRx = Store;
    fn get(&self, name: &str) -> crate::Result<&Store> {
        Ok(self.stores.get(name).unwrap())
    }

    fn list(&self) -> crate::Result<Vec<&Store>> {
        todo!()
    }
}

impl crate::SchemaTx for Schema {
    type StoreTx = Store;

    fn create(&mut self, to_create: StoreToCreate) -> crate::Result<()> {
        match to_create {
            StoreToCreate::Series { series: name, columns } => {
                assert!(self.stores.get(name.deref()).is_none());
                self.stores.insert(
                    name.deref().to_string(),
                    Store {
                        name,
                        kind: StoreKind::Series,
                        columns: columns
                            .into_iter()
                            .map(|c| Column { name: c.name, value: c.value, default: c.default })
                            .collect::<Vec<_>>(),
                    },
                );
            }

            StoreToCreate::Table { table: name, columns } => {
                assert!(self.stores.get(name.deref()).is_none());
                self.stores.insert(
                    name.deref().to_string(),
                    Store {
                        name,
                        kind: StoreKind::Table,
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

    fn create_if_not_exists(&mut self, to_create: StoreToCreate) -> crate::Result<()> {
        todo!()
    }

    fn drop(&mut self, name: &str) -> crate::Result<()> {
        todo!()
    }
}
