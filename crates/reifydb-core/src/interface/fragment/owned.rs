// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use super::{Fragment, SourceLocation, StatementColumn, StatementLine};

/// Owned fragment - owns all its data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OwnedFragment {
    /// No fragment information available
    None,
    
    /// Fragment from a SQL statement with position information
    Statement {
        text: String,
        line: StatementLine,
        column: StatementColumn,
        source: SourceLocation,
    },
    
    /// Fragment from internal/runtime code
    Internal {
        text: String,
        source: SourceLocation,
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
    
    fn source_location(&self) -> Option<&SourceLocation> {
        match self {
            OwnedFragment::None => None,
            OwnedFragment::Statement { source, .. } | OwnedFragment::Internal { source, .. } => Some(source),
        }
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
    
    /// Get debug location info (module, file, line) where the fragment was created
    pub fn location_info(&self) -> Option<(&str, &str, u32)> {
        self.source_location()
            .map(|loc| (loc.module.as_str(), loc.file.as_str(), loc.line))
    }
}

impl Default for OwnedFragment {
    fn default() -> Self {
        OwnedFragment::None
    }
}