// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/erikgrinaker/toydb (Apache 2 License).
// Original Apache 2 License Copyright (c) erikgrinaker 2024.

use crate::memory::test_memory_instance;
use testing::testscript;
use testscript::run;

#[test]
fn test_keys_are_case_sensitive() {
    run(
        test_memory_instance(),
        r#"
set a=1
get a
get A
---
"a" → "1"
"A" → None

set A=2
get a
get A
---
"a" → "1"
"A" → "2"

remove a
---
ok

get a
get A
---
"a" → None
"A" → "2"

scan
---
"A" → "2"

remove A
scan
---
ok
"#,
    )
}

#[test]
fn test_empty_keys_and_values_are_valid() {
    run(
        test_memory_instance(),
        r#"
set ""=""
get ""
scan
remove ""
---
"" → ""
"" → ""

scan
---
ok
"#,
    )
}


#[test]
fn test_null_keys_and_values_are_valid() {
	run(
		test_memory_instance(),
		r#"
set "\0"="\0"
get "\0"
scan
remove "\0"
---
"\x00" → "\x00"
"\x00" → "\x00"

scan
---
ok
"#,
	)
}

#[test]
fn test_unicode() {
	run(
		test_memory_instance(),
		r#"
set "👋"="👋"
get "👋"
scan
remove "👋"
---
"\xf0\x9f\x91\x8b" → "\xf0\x9f\x91\x8b"
"\xf0\x9f\x91\x8b" → "\xf0\x9f\x91\x8b"

scan
---
ok
"#,
	)
}
