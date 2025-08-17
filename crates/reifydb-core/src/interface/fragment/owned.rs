// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use super::{Fragment, StatementColumn, StatementLine};

/// Owned fragment - owns all its data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OwnedFragment {
    /// No fragment information available
    None,
    
    /// Fragment from a RQL statement with position information
    Statement {
        text: String,
        line: StatementLine,
        column: StatementColumn,
    },
    
    /// Fragment from internal/runtime code
    Internal {
        text: String,
    },
}

impl Fragment for OwnedFragment {
    fn value(&self) -> &str {
        match self {
            OwnedFragment::None => "",
            OwnedFragment::Statement { text, .. } | OwnedFragment::Internal { text, .. } => text,
        }
    }
    
    fn into_owned(self) -> OwnedFragment {
        self
    }
    
    fn position(&self) -> Option<(u32, u32)> {
        match self {
            OwnedFragment::Statement { line, column, .. } => Some((line.0, column.0)),
            _ => None,
        }
    }
}

impl OwnedFragment {
    /// Get the text value as an Option
    pub fn text(&self) -> Option<&str> {
        match self {
            OwnedFragment::None => None,
            OwnedFragment::Statement { text, .. } | OwnedFragment::Internal { text, .. } => Some(text),
        }
    }
    
    /// Create an internal fragment - useful for creating fragments from substrings
    pub fn internal(text: impl Into<String>) -> Self {
        OwnedFragment::Internal {
            text: text.into(),
        }
    }
    
    /// Create a testing fragment - returns an Internal fragment for test purposes
    #[cfg(test)]
    pub fn testing(text: impl Into<String>) -> Self {
        OwnedFragment::Internal {
            text: text.into(),
        }
    }
}

impl Default for OwnedFragment {
    fn default() -> Self {
        OwnedFragment::None
    }
}