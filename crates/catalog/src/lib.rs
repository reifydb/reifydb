// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use catalog::Catalog;
pub use column::*;
pub use error::Error;
pub use schema::*;
pub use store::*;

mod catalog;
mod column;
mod error;
mod schema;
mod store;

pub type Result<T> = std::result::Result<T, Error>;

pub trait CatalogRx {
    type SchemaRx: SchemaRx;

    fn get(&self, schema: &str) -> Result<&Self::SchemaRx>;

    fn list(&self) -> Result<Vec<&Self::SchemaRx>>;
}

pub trait CatalogTx: CatalogRx {
    type SchemaTx: SchemaTx;

    fn get_mut(&mut self, schema: &str) -> Result<&mut Self::SchemaRx>;

    fn create(&mut self, schema: &str) -> Result<()>;

    fn create_if_not_exists(&mut self, schema: &str) -> Result<()>;

    fn drop(&mut self, name: &str) -> Result<()>;
}
