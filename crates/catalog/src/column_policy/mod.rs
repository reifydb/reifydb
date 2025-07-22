// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// Column policy types have been moved to reifydb_core::interface::catalog::policy
// Please use those types instead of the ones that were previously defined here.

pub use reifydb_core::interface::{
    ColumnId, ColumnPolicy, ColumnPolicyId, ColumnPolicyKind, ColumnSaturationPolicy,
    DEFAULT_COLUMN_SATURATION_POLICY,
};

mod create;
mod layout;
mod list;
