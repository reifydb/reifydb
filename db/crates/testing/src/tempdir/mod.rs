// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{env, fs, path::Path};

pub fn temp_dir<F>(f: F) -> std::io::Result<()>
where
	F: FnOnce(&Path) -> std::io::Result<()>,
{
	let mut path = env::temp_dir();
	path.push(format!(
		"reifydb-{}-{}",
		std::process::id(),
		std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
	));

	fs::create_dir(&path)?;
	let result = f(&path);

	let _ = fs::remove_dir_all(&path);
	result
}
