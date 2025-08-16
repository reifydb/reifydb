// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{Fragment, OwnedFragment, SourceLocation, StatementColumn, StatementLine};

/// Borrowed fragment - zero-copy for parsing
#[derive(Debug, Clone)]
pub enum BorrowedFragment<'a> {
    /// No fragment information available
    None,
    
    /// Fragment from a SQL statement with position information
    Statement {
        text: &'a str,
        line: StatementLine,
        column: StatementColumn,
        source: SourceLocation,
    },
    
    /// Fragment from internal/runtime code
    Internal {
        text: &'a str,
        source: SourceLocation,
    },
}

impl<'a> Fragment for BorrowedFragment<'a> {
    fn value(&self) -> &str {
        match self {
            BorrowedFragment::None => "",
            BorrowedFragment::Statement { text, .. } | BorrowedFragment::Internal { text, .. } => text,
        }
    }
    
    fn into_owned(self) -> OwnedFragment {
        match self {
            BorrowedFragment::None => OwnedFragment::None,
            BorrowedFragment::Statement { text, line, column, source } => {
                OwnedFragment::Statement {
                    text: text.to_string(),
                    line,
                    column,
                    source,
                }
            }
            BorrowedFragment::Internal { text, source } => {
                OwnedFragment::Internal {
                    text: text.to_string(),
                    source,
                }
            }
        }
    }
    
    fn source_location(&self) -> Option<&SourceLocation> {
        match self {
            BorrowedFragment::None => None,
            BorrowedFragment::Statement { source, .. } | BorrowedFragment::Internal { source, .. } => Some(source),
        }
    }
    
    fn position(&self) -> Option<(u32, u32)> {
        match self {
            BorrowedFragment::Statement { line, column, .. } => Some((line.0, column.0)),
            _ => None,
        }
    }
}

impl<'a> BorrowedFragment<'a> {
    /// Create a new Statement fragment
    pub fn new_statement(text: &'a str, line: StatementLine, column: StatementColumn) -> Self {
        BorrowedFragment::Statement {
            text,
            line,
            column,
            source: SourceLocation::from_static(
                module_path!(),
                file!(),
                line!(),
            ),
        }
    }
    
    /// Create a new Internal fragment
    pub fn new_internal(text: &'a str) -> Self {
        BorrowedFragment::Internal {
            text,
            source: SourceLocation::from_static(
                module_path!(),
                file!(),
                line!(),
            ),
        }
    }
}