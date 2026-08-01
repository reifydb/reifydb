// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Columnar storage engine: the immutable on-disk column representation plus the read-time machinery the engine
//! queries it through. A column's encoded bytes, stats and bitmap are produced as one unit and never updated
//! piecewise - stats that no longer describe their values silently corrupt every kernel that reads them to skip work.

pub mod bucket;
pub mod compress;
pub mod compute;
pub mod encoding;
pub mod error;
pub mod persist;
pub mod predicate;
pub mod reader;
pub mod scan;
pub mod selection;
pub mod snapshot;
