// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::schema::{Schema, SchemaMut};
use base::schema::SchemaName;

pub trait Catalog {
    type Schema: Schema;

    fn get(&self, schema: impl AsRef<str>) -> crate::Result<&Self::Schema>;

    fn list(&self) -> crate::Result<Vec<&Self::Schema>>;
}

pub trait CatalogMut: Catalog {
    type SchemaMut: SchemaMut;

    fn get_mut(&mut self, schema: impl AsRef<str>) -> crate::Result<&mut Self::Schema>;

    fn create(&mut self, schema: impl AsRef<SchemaName>) -> crate::Result<()>;

    fn create_if_not_exists(&mut self, schema: impl AsRef<SchemaName>) -> crate::Result<()>;

    fn drop(&mut self, name: impl AsRef<str>) -> crate::Result<()>;
}
