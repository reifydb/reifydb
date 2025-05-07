// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::expression::Expression;

#[derive(Debug)]
pub enum Node {
    Project { input: Box<Node>, expressions: Vec<Expression> },
    Scan { /*source: Source(Table|Buffer|Index)...*/ filter: Vec<Expression> },
}
