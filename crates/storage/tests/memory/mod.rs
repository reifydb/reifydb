// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::testscript::test::Runner;
use storage::Memory;

mod key;
mod point;
mod scan;
mod scan_prefix;

pub(crate) fn test_memory_instance() -> Runner<Memory> {
    Runner::new(Memory::default())
}
