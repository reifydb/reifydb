// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#[derive(Debug)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug)]
pub struct SortKey {
    pub column: String,
    pub direction: SortDirection,
}
