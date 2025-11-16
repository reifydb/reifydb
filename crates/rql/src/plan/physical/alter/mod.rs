// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod flow;
mod sequence;
mod table;
mod view;

pub use flow::{AlterFlowAction, AlterFlowNode};
pub use table::AlterTableNode;
pub use view::AlterViewNode;
