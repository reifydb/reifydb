// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Span;

#[derive(Debug)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug)]
pub struct OrderKey {
    pub column: Span,
    pub direction: OrderDirection,
}
