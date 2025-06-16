// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::DepSchema;
use std::collections::HashMap;

#[derive(Debug)]
pub struct DepCatalog {
    schema: HashMap<String, DepSchema>,
}

impl DepCatalog {
    pub fn new() -> Self {
        Self { schema: HashMap::new() }
    }
}

impl crate::DepCatalogRx for DepCatalog {
    type SchemaRx = DepSchema;
    fn get(&self, schema: &str) -> crate::Result<&DepSchema> {
        Ok(self.schema.get(schema).unwrap())
    }

    fn list(&self) -> crate::Result<Vec<&DepSchema>> {
        todo!()
    }
}

impl crate::DepCatalogTx for DepCatalog {
    type SchemaTx = DepSchema;

    fn get_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaRx> {
        Ok(self.schema.get_mut(schema).unwrap())
    }

    fn create(&mut self, schema: &str) -> crate::Result<()> {
        // assert!(self.schema.get(schema).is_none()); // FIXME
        self.schema.insert(schema.clone().into(), DepSchema::new(schema.to_string()));
        Ok(())
    }

    fn create_if_not_exists(&mut self, schema: &str) -> crate::Result<()> {
        todo!()
    }

    fn drop(&mut self, name: &str) -> crate::Result<()> {
        todo!()
    }
}
