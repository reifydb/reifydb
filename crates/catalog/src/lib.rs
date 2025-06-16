// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use catalog::DepCatalog;
pub use dep::column::*;
pub use error::Error;
pub use dep::schema::*;
pub use dep::store::*;

mod catalog;
mod error;
mod dep;
mod column;
mod schema;
mod table;

pub type Result<T> = std::result::Result<T, Error>;

pub trait DepCatalogRx {
    type SchemaRx: DepSchemaRx;

    fn get(&self, schema: &str) -> Result<&Self::SchemaRx>;

    fn list(&self) -> Result<Vec<&Self::SchemaRx>>;
}

pub trait DepCatalogTx: DepCatalogRx {
    type SchemaTx: DepSchemaTx;

    fn get_mut(&mut self, schema: &str) -> Result<&mut Self::SchemaRx>;

    fn create(&mut self, schema: &str) -> Result<()>;

    fn create_if_not_exists(&mut self, schema: &str) -> Result<()>;

    fn drop(&mut self, name: &str) -> Result<()>;
}
