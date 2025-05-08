// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/erikgrinaker/toydb (Apache 2 License).
// Original Apache 2 License Copyright (c) erikgrinaker 2024.

use crate::memory::test_memory_instance;
use testing::testscript::run;

#[test]
fn test_missing_key_in_empty_storage() {
    run(
        test_memory_instance(),
        r#"
get a
---
"a" → None
"#,
    )
}

#[test]
fn test_set_get_values() {
    run(
        test_memory_instance(),
        r#"
# Write a couple of keys.
set a=1
set b=2
---
ok

# Reading the value back should return it. An unknown key should return None.
get a
get b
get c
---
"a" → "1"
"b" → "2"
"c" → None
"#,
    )
}

#[test]
fn test_replace_value() {
    run(
        test_memory_instance(),
        r#"
get a

set a="SomeValue"
get a
scan

set a="AnotherValue"
get a
scan
---
"a" → None
"a" → "SomeValue"
"a" → "SomeValue"
"a" → "AnotherValue"
"a" → "AnotherValue"
"#,
    )
}

#[test]
fn test_remove_key() {
    run(
        test_memory_instance(),
        r#"
# Removing a key should remove it, but not affect other keys.
set a=1
set b=2
remove a
get a
get b
---
"a" → None
"b" → "2"

# Removes are idempotent.
remove a
get a
---
"a" → None

# Setting a removed key works fine.
set a=3
get a
---
"a" → "3"
"#,
    )
}
