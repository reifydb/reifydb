// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ValueKind;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug)]
pub struct RowMeta {
    pub label: String,
    pub value: ValueKind,
}

impl Display for RowMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.label)
    }
}
