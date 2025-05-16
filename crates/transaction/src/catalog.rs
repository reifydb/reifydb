// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::schema::{SchemaRx, SchemaTx};

pub trait CatalogRx {
    type SchemaRx: SchemaRx;

    fn get(&self, schema: &str) -> crate::Result<&Self::SchemaRx>;

    fn list(&self) -> crate::Result<Vec<&Self::SchemaRx>>;
}

pub trait CatalogTx: CatalogRx {
    type SchemaTx: SchemaTx;

    fn get_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaRx>;

    fn create(&mut self, schema: &str) -> crate::Result<()>;

    fn create_if_not_exists(&mut self, schema: &str) -> crate::Result<()>;

    fn drop(&mut self, name: &str) -> crate::Result<()>;
}
