// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{env, fs, path::Path};

use uuid::Uuid;

pub fn temp_dir<F>(f: F) -> std::io::Result<()>
where
	F: FnOnce(&Path) -> std::io::Result<()>,
{
	let mut path = env::temp_dir();
	path.push(format!("reifydb-{}", Uuid::new_v4().to_string()));

	fs::create_dir(&path)?;
	let result = f(&path);

	let _ = fs::remove_dir_all(&path);
	result
}
