// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::fmt::{Display, Formatter};
use std::ops::Deref;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ColumnName(String);

impl Display for ColumnName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl ColumnName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl Deref for ColumnName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<str> for ColumnName {
    fn eq(&self, other: &str) -> bool {
        self.0.as_str() == other
    }
}
