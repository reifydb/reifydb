// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	cmp::Ordering,
	fmt::{Display, Formatter},
};

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
    type SubFragment = OwnedFragment;
    
    fn value(&self) -> &str {
        match self {
            OwnedFragment::None => "",
            OwnedFragment::Statement { text, .. } | OwnedFragment::Internal { text, .. } => text,
        }
    }
    
    fn line(&self) -> StatementLine {
        match self {
            OwnedFragment::Statement { line, .. } => *line,
            _ => StatementLine(1),
        }
    }
    
    fn column(&self) -> StatementColumn {
        match self {
            OwnedFragment::Statement { column, .. } => *column,
            _ => StatementColumn(0),
        }
    }
    
    fn into_owned(self) -> OwnedFragment {
        self
    }
    
    fn sub_fragment(&self, offset: usize, length: usize) -> OwnedFragment {
        let text = self.value();
        let end = std::cmp::min(offset + length, text.len());
        let sub_text = if offset < text.len() {
            &text[offset..end]
        } else {
            ""
        };
        
        match self {
            OwnedFragment::None => OwnedFragment::None,
            OwnedFragment::Statement { line, column, .. } => OwnedFragment::Statement {
                text: sub_text.to_string(),
                line: *line,
                column: StatementColumn(column.0 + offset as u32),
            },
            OwnedFragment::Internal { .. } => OwnedFragment::Internal {
                text: sub_text.to_string(),
            },
        }
    }
}

impl OwnedFragment {
    /// Create an internal fragment - useful for creating fragments from substrings
    pub fn internal(text: impl Into<String>) -> Self {
        OwnedFragment::Internal {
            text: text.into(),
        }
    }
    
    /// Create a testing fragment - returns a Statement fragment for test purposes
    pub fn testing(text: impl Into<String>) -> Self {
        OwnedFragment::Statement {
            text: text.into(),
            line: StatementLine(1),
            column: StatementColumn(0),
        }
    }
    
    /// Create an empty testing fragment
    pub fn testing_empty() -> Self {
        Self::testing("")
    }
    
    /// Merge multiple fragments (in any order) into one encompassing fragment
    pub fn merge_all(
        fragments: impl IntoIterator<Item = OwnedFragment>,
    ) -> OwnedFragment {
        let mut fragments: Vec<OwnedFragment> = fragments.into_iter().collect();
        assert!(!fragments.is_empty());
        
        fragments.sort();
        
        let first = fragments.first().unwrap();
        
        let mut text = String::with_capacity(
            fragments.iter().map(|f| f.value().len()).sum(),
        );
        for fragment in &fragments {
            text.push_str(fragment.value());
        }
        
        match first {
            OwnedFragment::None => OwnedFragment::None,
            OwnedFragment::Statement { line, column, .. } => OwnedFragment::Statement {
                text,
                line: *line,
                column: *column,
            },
            OwnedFragment::Internal { .. } => OwnedFragment::Internal {
                text,
            },
        }
    }
    
    /// Compatibility: expose fragment field for Fragment compatibility
    pub fn fragment(&self) -> &str {
        self.value()
    }
}

impl Default for OwnedFragment {
    fn default() -> Self {
        OwnedFragment::None
    }
}

impl AsRef<str> for OwnedFragment {
    fn as_ref(&self) -> &str {
        self.value()
    }
}

impl Display for OwnedFragment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.value(), f)
    }
}

impl PartialOrd for OwnedFragment {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OwnedFragment {
    fn cmp(&self, other: &Self) -> Ordering {
        self.column().cmp(&other.column()).then(self.line().cmp(&other.line()))
    }
}

impl Eq for OwnedFragment {}