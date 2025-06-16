// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ValueKind;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
pub struct RowId(pub u64);

impl Deref for RowId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<u64> for RowId {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(other)
    }
}




// #[derive(Debug, PartialEq)]
// pub struct Table {
//     pub id: TableId,
//     pub schema: SchemaId,
//     pub name: String,
// }
// 
// #[derive(Debug)]
// pub struct TableToCreate {
//     pub id: TableId,
//     pub schema: SchemaId,
//     pub name: String,
// }
// 
