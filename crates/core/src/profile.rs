// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Cross-crate identifier for profile categories. The full `ProfileCategory` enum lives in `reifydb-profiler`; this
//! crate carries only the u8 newtype so that `reifydb-metric` can key `MetricId::Profile(ProfileCategoryId)` without
//! depending on the profiler crate.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProfileCategoryId(pub u8);
