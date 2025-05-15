// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0
use crate::memory::test_memory_instance;
use testing::testscript;
use testscript::run;

#[test]
fn test_scan_prefix() {
    run(
        test_memory_instance(),
        r#"
# Tests prefix scans.

# Set up an initial dataset of keys with overlapping or adjacent prefixes.
set a=1
set b=2
set ba=21
set bb=22
set "b\xff"=2f
set "b\xff\x00"=2f0
set "b\xffb"=2fb
set "b\xff\xff"=2ff
set c=3
set "\xff"=f
set "\xff\xff"=ff
set "\xff\xff\xff"=fff
set "\xff\xff\xff\xff"=ffff
scan
---
"a" → "1"
"b" → "2"
"ba" → "21"
"bb" → "22"
"b\xff" → "2f"
"b\xff\x00" → "2f0"
"b\xffb" → "2fb"
"b\xff\xff" → "2ff"
"c" → "3"
"\xff" → "f"
"\xff\xff" → "ff"
"\xff\xff\xff" → "fff"
"\xff\xff\xff\xff" → "ffff"

# An empty prefix returns everything.
scan_prefix ""
---
"a" → "1"
"b" → "2"
"ba" → "21"
"bb" → "22"
"b\xff" → "2f"
"b\xff\x00" → "2f0"
"b\xffb" → "2fb"
"b\xff\xff" → "2ff"
"c" → "3"
"\xff" → "f"
"\xff\xff" → "ff"
"\xff\xff\xff" → "fff"
"\xff\xff\xff\xff" → "ffff"

# A missing prefix returns nothing.
scan_prefix bx
---
ok

# Various prefixes under b. In particular, this tests that the bounds generation
# handles 0xff bytes properly.
scan_prefix b
---
"b" → "2"
"ba" → "21"
"bb" → "22"
"b\xff" → "2f"
"b\xff\x00" → "2f0"
"b\xffb" → "2fb"
"b\xff\xff" → "2ff"

scan_prefix bb
---
"bb" → "22"

scan_prefix "b\xff"
---
"b\xff" → "2f"
"b\xff\x00" → "2f0"
"b\xffb" → "2fb"
"b\xff\xff" → "2ff"

scan_prefix "b\xff\x00"
---
"b\xff\x00" → "2f0"

scan_prefix "b\xff\xff"
---
"b\xff\xff" → "2ff"

# Chains of \xff prefixes.
scan_prefix "\xff"
---
"\xff" → "f"
"\xff\xff" → "ff"
"\xff\xff\xff" → "fff"
"\xff\xff\xff\xff" → "ffff"

scan_prefix "\xff\xff"
---
"\xff\xff" → "ff"
"\xff\xff\xff" → "fff"
"\xff\xff\xff\xff" → "ffff"

scan_prefix "\xff\xff\xff"
---
"\xff\xff\xff" → "fff"
"\xff\xff\xff\xff" → "ffff"

scan_prefix "\xff\xff\xff\xff"
---
"\xff\xff\xff\xff" → "ffff"

scan_prefix "\xff\xff\xff\xff\xff"
---
ok
"#,
    )
}
