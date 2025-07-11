// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Frame, FrameLayout};
use reifydb_core::BitVec;

pub(crate) use compile::compile;
pub(crate) use filter::FilterNode;
pub(crate) use project::ProjectNode;
pub(crate) use scan::ScanFrameNode;
pub(crate) use take::TakeNode;

mod aggregate;
mod compile;
mod filter;
mod join;
mod order;
mod project;
mod scan;
mod take;

#[derive(Debug)]
pub(crate) struct Batch {
    pub frame: Frame,
    pub mask: BitVec,
}

pub(crate) trait ExecutionPlan {
    fn next(&mut self) -> crate::Result<Option<Batch>>;
    fn layout(&self) -> Option<FrameLayout>;
}
