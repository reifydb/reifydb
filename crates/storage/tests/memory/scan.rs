// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/erikgrinaker/toydb (Apache 2 License).
// Original Apache 2 License Copyright (c) erikgrinaker 2024.

use crate::memory::test_memory_instance;
use testing::testscript;
use testscript::run;

#[test]
fn test_scan_forward() {
    run(
        test_memory_instance(),
        r#"
# init
set a=1
set b=2
set ba=21
set bb=22
set c=3
set C=3
---
ok

# test
scan
---
"C" → "3"
"a" → "1"
"b" → "2"
"ba" → "21"
"bb" → "22"
"c" → "3"
"#,
    )
}

#[test]
fn test_scan_reverse() {
    run(
        test_memory_instance(),
        r#"
# init
set a=1
set b=2
set ba=21
set bb=22
set c=3
set C=3
---
ok

# test
scan reverse=true
---
"c" → "3"
"bb" → "22"
"ba" → "21"
"b" → "2"
"a" → "1"
"C" → "3"
"#,
    )
}

#[test]
fn test_scan_inclusive_exclusive() {
    run(
        test_memory_instance(),
        r#"
# init
set a=1
set b=2
set ba=21
set bb=22
set c=3
set C=3
---
ok

# test
scan b..bb
---
"b" → "2"
"ba" → "21"

scan "b..=bb"
---
"b" → "2"
"ba" → "21"
"bb" → "22"
"#,
    )
}

#[test]
fn test_scan_open_ranges() {
    run(
        test_memory_instance(),
        r#"
# init
set a=1
set b=2
set ba=21
set bb=22
set c=3
set C=3
---
ok

# test
scan bb..
---
"bb" → "22"
"c" → "3"

scan "..=b"
---
"C" → "3"
"a" → "1"
"b" → "2"
"#,
    )
}