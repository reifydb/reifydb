// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#[cfg(test)]
use crate::Catalog;
#[cfg(test)]
use crate::schema::SchemaToCreate;
#[cfg(test)]
use reifydb_core::catalog::Schema;
#[cfg(test)]
use reifydb_storage::memory::Memory;
#[cfg(test)]
use reifydb_transaction::Tx;

#[cfg(test)]
pub fn create_schema(tx: &mut impl Tx<Memory, Memory>, schema: &str) -> Schema {
    Catalog::create_schema(tx, SchemaToCreate { schema_span: None, name: schema.to_string() })
        .unwrap()
}

#[cfg(test)]
pub fn ensure_test_schema(tx: &mut impl Tx<Memory, Memory>) -> Schema {
    if let Some(result) = Catalog::get_schema_by_name(tx, "test_schema").unwrap() {
        return result;
    }
    create_schema(tx, "test_schema")
}
