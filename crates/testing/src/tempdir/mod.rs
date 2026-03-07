// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{env, fs, io, path::Path};

use uuid::Uuid;

pub fn temp_dir<F>(f: F) -> io::Result<()>
where
	F: FnOnce(&Path) -> io::Result<()>,
{
	let mut path = env::temp_dir();
	path.push(format!("reifydb-{}", Uuid::new_v4().to_string()));

	fs::create_dir(&path)?;
	let result = f(&path);

	let _ = fs::remove_dir_all(&path);
	result
}
