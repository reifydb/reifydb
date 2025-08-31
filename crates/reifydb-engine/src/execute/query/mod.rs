// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod aggregate;
pub mod compile;
pub mod extend;
pub mod filter;
pub mod inline;
pub mod join_inner;
pub mod join_left;
pub mod join_natural;
mod layout;
pub mod map;
pub mod sort;
pub mod table_scan;
pub mod table_virtual_scan;
pub mod take;
pub mod view_scan;
