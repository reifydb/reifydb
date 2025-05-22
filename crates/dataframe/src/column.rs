// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use base::ColumnValues;

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data: ColumnValues,
}
