// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = reifydb_sql::transpile(data);
});
