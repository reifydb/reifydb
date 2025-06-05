// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Schema;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Catalog {
    schema: HashMap<String, Schema>,
}

impl Catalog {
    pub fn new() -> Self {
        Self { schema: HashMap::new() }
    }
}

impl crate::CatalogRx for Catalog {
    type SchemaRx = Schema;
    fn get(&self, schema: &str) -> crate::Result<&Schema> {
        Ok(self.schema.get(schema).unwrap())
    }

    fn list(&self) -> crate::Result<Vec<&Schema>> {
        todo!()
    }
}

impl crate::CatalogTx for Catalog {
    type SchemaTx = Schema;

    fn get_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaRx> {
        Ok(self.schema.get_mut(schema).unwrap())
    }

    fn create(&mut self, schema: &str) -> crate::Result<()> {
        // assert!(self.schema.get(schema).is_none()); // FIXME
        self.schema.insert(schema.clone().into(), Schema::new(schema.to_string()));
        Ok(())
    }

    fn create_if_not_exists(&mut self, schema: &str) -> crate::Result<()> {
        todo!()
    }

    fn drop(&mut self, name: &str) -> crate::Result<()> {
        todo!()
    }
}
