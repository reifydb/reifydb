// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::function::Functions;
use reifydb_catalog::table::Table;

pub struct ExecutionContext {
    pub functions: Functions,
    pub table: Option<Table>,
}

impl ExecutionContext {
    pub fn new(functions: Functions) -> Self {
        Self { functions, table: None }
    }

    pub fn with_table(functions: Functions, table: Table) -> Self {
        Self { functions, table: Some(table) }
    }
}
