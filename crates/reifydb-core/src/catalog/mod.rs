// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod materialized;
pub mod queries;
pub mod versioned;


pub use crate::interface::change::{TransactionalChanges, Change, OperationType};
pub use materialized::MaterializedCatalog;
