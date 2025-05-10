// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::svl::EngineInner;
use crate::svl::catalog::{Catalog, CatalogMut};
use crate::svl::schema::{Schema, SchemaMut};
use base::expression::Expression;
use base::{Key, Row, RowIter, Value};
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

pub struct Transaction<'a> {
    guard: RwLockReadGuard<'a, EngineInner>,
}

impl<'a> Transaction<'a> {
    pub fn new(guard: RwLockReadGuard<'a, EngineInner>) -> Self {
        Self { guard }
    }
}

impl<'a> crate::Transaction for Transaction<'a> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<Self::Catalog> {
        Ok(Catalog {})
    }

    fn schema(&self) -> crate::Result<Option<Self::Schema>> {
        Ok(Some(Schema {}))
    }

    fn get(&self, table: &str, ids: &[Key]) -> crate::Result<Vec<Row>> {
        unreachable!()
    }

    fn scan(&self, store: impl AsRef<str>, filter: Option<Expression>) -> crate::Result<RowIter> {
        Ok(Box::new(
            vec![
                vec![Value::Int2(1), Value::Text("Alice".to_string()), Value::Boolean(true)],
                vec![Value::Int2(2), Value::Text("Bob".to_string()), Value::Boolean(false)],
            ]
            .into_iter(),
        ))
    }
}

pub struct TransactionMut<'a> {
    guard: RwLockWriteGuard<'a, EngineInner>,
}

impl<'a> TransactionMut<'a> {
    pub fn new(guard: RwLockWriteGuard<'a, EngineInner>) -> Self {
        Self { guard }
    }
}

impl<'a> crate::Transaction for TransactionMut<'a> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<Self::Catalog> {
        todo!()
    }

    fn schema(&self) -> crate::Result<Option<Self::Schema>> {
        todo!()
    }

    fn get(&self, table: &str, ids: &[Key]) -> crate::Result<Vec<Row>> {
        todo!()
    }

    fn scan(&self, store: impl AsRef<str>, filter: Option<Expression>) -> crate::Result<RowIter> {
        todo!()
    }
}

impl<'a> crate::TransactionMut for TransactionMut<'a> {
    type CatalogMut = CatalogMut;
    type SchemaMut = SchemaMut;

    fn catalog_mut(&self) -> crate::Result<Self::CatalogMut> {
        todo!()
    }

    fn schema_mut(&self) -> crate::Result<Option<Self::SchemaMut>> {
        todo!()
    }

    fn commit(self) -> crate::Result<()> {
        todo!()
    }

    fn rollback(self) -> crate::Result<()> {
        todo!()
    }
}
