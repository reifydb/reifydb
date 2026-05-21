// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::AtomicU64;

pub static SQLITE_READ_NANOS: AtomicU64 = AtomicU64::new(0);
pub static SQLITE_READ_COUNT: AtomicU64 = AtomicU64::new(0);
