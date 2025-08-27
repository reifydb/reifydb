// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod sequence;
mod table;
mod view;

pub use table::{AlterTableOperation, AlterTablePlan};
pub use view::{AlterViewOperation, AlterViewPlan};
