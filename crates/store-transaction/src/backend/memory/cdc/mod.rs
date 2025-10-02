// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file
use reifydb_core::interface::CdcStore;

use crate::backend::memory::MemoryBackend;

mod count;
mod get;
mod range;
mod scan;

pub use range::CdcRangeIter;
pub use scan::CdcScanIter;

impl CdcStore for MemoryBackend {}
