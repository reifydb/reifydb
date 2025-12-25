// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Source-specific compilation implementations

mod flow_scan;
mod inline_data;
mod table_scan;
mod view_scan;

pub(crate) use flow_scan::FlowScanCompiler;
pub(crate) use inline_data::InlineDataCompiler;
pub(crate) use table_scan::TableScanCompiler;
pub(crate) use view_scan::ViewScanCompiler;
