// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SeparatorToken {
    Semicolon, // ;
    Comma,     // ,
    NewLine,
}
