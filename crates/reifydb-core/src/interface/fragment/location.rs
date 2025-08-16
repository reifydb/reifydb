// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

/// Represents the source location where a fragment was captured
/// This is debug information showing where in the Rust code the fragment was created
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceLocation {
    /// Module path where the fragment was captured (from module_path!())
    pub module: String,
    /// File path where the fragment was captured (from file!())
    pub file: String,
    /// Line number where the fragment was captured (from line!())
    pub line: u32,
}

impl SourceLocation {
    /// Create a new SourceLocation with the given values
    pub fn new(module: String, file: String, line: u32) -> Self {
        Self { module, file, line }
    }
    
    /// Create a SourceLocation from static strings (used by macros)
    pub fn from_static(module: &'static str, file: &'static str, line: u32) -> Self {
        Self {
            module: module.to_string(),
            file: file.to_string(),
            line,
        }
    }
}