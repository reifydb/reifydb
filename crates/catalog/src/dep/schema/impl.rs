// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{DepColumn, DepStore, DepStoreToCreate};
use reifydb_core::StoreKind;
use std::collections::HashMap;
use std::ops::Deref;

#[derive(Debug)]
pub struct DepSchema {
    name: String,
    stores: HashMap<String, DepStore>,
}

impl DepSchema {
    pub fn new(name: String) -> Self {
        Self { name, stores: HashMap::new() }
    }
}

impl crate::DepSchemaRx for DepSchema {
    type StoreRx = DepStore;
    fn get(&self, name: &str) -> crate::Result<&DepStore> {
        Ok(self.stores.get(name).unwrap())
    }

    fn list(&self) -> crate::Result<Vec<&DepStore>> {
        todo!()
    }
}

impl crate::DepSchemaTx for DepSchema {
    type StoreTx = DepStore;

    fn create(&mut self, to_create: DepStoreToCreate) -> crate::Result<()> {
        match to_create {
            DepStoreToCreate::DeferredView { view: name, columns } => {
                assert!(self.stores.get(name.deref()).is_none());
                self.stores.insert(
                    name.deref().to_string(),
                    DepStore {
                        name,
                        kind: StoreKind::Table,
                        columns: columns
                            .into_iter()
                            .map(|c| DepColumn::new(c.name, c.value, c.policies))
                            .collect::<Vec<_>>(),
                    },
                );
            },
        
            DepStoreToCreate::Series { series: name, columns } => {
                assert!(self.stores.get(name.deref()).is_none());
                self.stores.insert(
                    name.deref().to_string(),
                    DepStore {
                        name,
                        kind: StoreKind::Series,
                        columns: columns
                            .into_iter()
                            .map(|c| DepColumn::new(c.name, c.value, vec![]))
                            .collect::<Vec<_>>(),
                    },
                );
            }

            DepStoreToCreate::Table { table: name, columns } => {
                assert!(self.stores.get(name.deref()).is_none());
                self.stores.insert(
                    name.deref().to_string(),
                    DepStore {
                        name,
                        kind: StoreKind::Table,
                        columns: columns
                            .into_iter()
                            .map(|c| DepColumn::new(c.name, c.value, c.policies))
                            .collect::<Vec<_>>(),
                    },
                );
            },
        }
        Ok(())
    }

    fn create_if_not_exists(&mut self, to_create: DepStoreToCreate) -> crate::Result<()> {
        todo!()
    }

    fn drop(&mut self, name: &str) -> crate::Result<()> {
        todo!()
    }
}
