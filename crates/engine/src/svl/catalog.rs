// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::svl::schema::{Schema, SchemaMut};
use base::schema::Store;

pub struct Catalog {}

impl crate::Catalog for Catalog {
    type Schema = Schema;

    fn get(&self, name: impl AsRef<str>) -> crate::Result<Option<Self::Schema>> {
        Ok(Some(Schema {}))
    }

    fn list(&self) -> crate::Result<Vec<Self::Schema>> {
        todo!()
    }
}

pub struct CatalogMut {}

impl crate::Catalog for CatalogMut {
    type Schema = SchemaMut;

    fn get(&self, name: impl AsRef<str>) -> crate::Result<Option<Self::Schema>> {
        todo!()
    }

    fn list(&self) -> crate::Result<Vec<Self::Schema>> {
        todo!()
    }
}

impl crate::CatalogMut for CatalogMut {
    fn create(&self, store: Store) -> crate::Result<()> {
        todo!()
    }

    fn create_if_not_exists(&self, store: Store) -> crate::Result<()> {
        todo!()
    }

    fn drop(&self, name: impl AsRef<str>) -> crate::Result<()> {
        todo!()
    }
}
