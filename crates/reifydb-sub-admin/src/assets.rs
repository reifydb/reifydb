// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, sync::OnceLock};

pub struct EmbeddedFile {
	pub content: &'static [u8],
	pub mime_type: &'static str,
}

static EMBEDDED_FILES: OnceLock<HashMap<&'static str, EmbeddedFile>> =
	OnceLock::new();

include!(concat!(env!("OUT_DIR"), "/webapp/asset_manifest.rs"));

fn init_embedded_files() -> HashMap<&'static str, EmbeddedFile> {
	let mut files = HashMap::new();

	for (path, content, mime_type) in ASSETS {
		files.insert(
			*path,
			EmbeddedFile {
				content,
				mime_type,
			},
		);

		// Also map /assets/* paths to app/* files
		// This handles the mismatch between HTML references and actual
		// file locations
		if path.starts_with("app/") {
			let asset_path = path.replacen("app/", "assets/", 1);
			files.insert(
				// We need to leak the string to get a 'static
				// lifetime
				Box::leak(asset_path.into_boxed_str()),
				EmbeddedFile {
					content,
					mime_type,
				},
			);
		}
	}

	files
}

pub fn get_embedded_file(path: &str) -> Option<&'static EmbeddedFile> {
	let files = EMBEDDED_FILES.get_or_init(init_embedded_files);

	// Remove leading slash if present
	let path = path.strip_prefix('/').unwrap_or(path);

	// Default to index.html for root
	let path = if path.is_empty() {
		"index.html"
	} else {
		path
	};

	files.get(path)
}
