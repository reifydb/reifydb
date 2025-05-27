// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::path::Path;
use std::{env, fs, panic};

pub fn temp_dir<F>(f: F)
where
    F: FnOnce(&Path) + panic::UnwindSafe,
{
    let mut path = env::temp_dir();
    path.push(format!("reifydb_{}", uuid::Uuid::new_v4()));

    fs::create_dir(&path).unwrap();
    let result = panic::catch_unwind(|| {
        f(&path);
    });

    let _ = fs::remove_dir_all(&path);
    if let Err(payload) = result {
        panic::resume_unwind(payload);
    }
}
