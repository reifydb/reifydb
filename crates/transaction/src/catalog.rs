// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::schema::{Schema, SchemaMut};

pub trait Catalog {
    type Schema: Schema;

    fn get(&self, schema: &str) -> crate::Result<&Self::Schema>;

    fn list(&self) -> crate::Result<Vec<&Self::Schema>>;
}

pub trait CatalogMut: Catalog {
    type SchemaMut: SchemaMut;

    fn get_mut(&mut self, schema: &str) -> crate::Result<&mut Self::Schema>;

    fn create(&mut self, schema: &str) -> crate::Result<()>;

    fn create_if_not_exists(&mut self, schema: &str) -> crate::Result<()>;

    fn drop(&mut self, name: &str) -> crate::Result<()>;
}
