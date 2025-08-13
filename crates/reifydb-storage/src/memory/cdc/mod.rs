// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod count;
mod get;
mod range;
mod scan;

use crate::memory::Memory;
use reifydb_core::interface::CdcStorage;

impl CdcStorage for Memory {}