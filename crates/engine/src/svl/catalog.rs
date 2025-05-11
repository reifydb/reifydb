// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::svl::schema::Schema;
use std::collections::HashMap;
use std::ops::Deref;

pub struct Catalog {
    schema: HashMap<String, Schema>,
}

impl Catalog {
    pub fn new() -> Self {
        Self { schema: HashMap::new() }
    }
}

impl crate::Catalog for Catalog {
    type Schema = Schema;
    fn get(&self, schema: impl AsRef<str>) -> crate::Result<&Schema> {
        Ok(self.schema.get(schema.as_ref()).unwrap())
    }

    fn list(&self) -> crate::Result<Vec<&Schema>> {
        todo!()
    }
}

impl crate::CatalogMut for Catalog {
    type SchemaMut = Schema;

    fn get_mut(&mut self, schema: impl AsRef<str>) -> crate::Result<&mut Self::Schema> {
        Ok(self.schema.get_mut(schema.as_ref()).unwrap())
    }

    fn create(&mut self, schema: base::schema::Schema) -> crate::Result<()> {
        assert!(self.schema.get(schema.name.deref()).is_none()); // FIXME
        self.schema.insert(schema.name.to_string(), Schema::new(schema.name));
        Ok(())
    }

    fn create_if_not_exists(&mut self, schema: base::schema::Schema) -> crate::Result<()> {
        todo!()
    }

    fn drop(&mut self, name: impl AsRef<str>) -> crate::Result<()> {
        todo!()
    }
}
