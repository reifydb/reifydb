// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::expression::Expression;

#[derive(Debug)]
pub enum Node {
    Project { expressions: Vec<Expression> },
    Scan { filter: Vec<Expression> },
}
