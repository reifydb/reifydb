// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod flow;
mod sequence;
mod table;
mod view;

pub use flow::{AlterFlowAction, AlterFlowNode};
pub use table::AlterTableNode;
pub use view::AlterViewNode;
