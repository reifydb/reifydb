// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Frame, FrameLayout};
use reifydb_core::BitVec;

pub(crate) use compile::compile;
pub(crate) use filter::FilterNode;
pub(crate) use limit::LimitNode;
pub(crate) use project::ProjectNode;
pub(crate) use scan::ScanFrameNode;

mod aggregate;
mod compile;
mod filter;
mod limit;
mod project;
mod scan;
mod order;
mod join;

#[derive(Debug)]
pub(crate) struct Batch {
    pub frame: Frame,
    pub mask: BitVec,
}

pub(crate) trait ExecutionPlan {
    fn next(&mut self) -> crate::Result<Option<Batch>>;
    fn layout(&self) -> Option<FrameLayout>;
}
