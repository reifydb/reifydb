// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{env, fs, io::Write, path::Path};

use fs::create_dir_all;

fn main() {
	// Only rebuild if webapp source changes
	println!("cargo:rerun-if-changed=webapp/src");
	println!("cargo:rerun-if-changed=webapp/dist");
	println!("cargo:rerun-if-changed=webapp/package.json");

	let out_dir = env::var("OUT_DIR").unwrap();
	let webapp_dist = Path::new("webapp/dist");
	let dest_path = Path::new(&out_dir).join("webapp");

	if webapp_dist.exists() {
		println!("cargo:warning=Found webapp/dist directory, copying to build output");

		create_dir_all(&dest_path).expect("Failed to create webapp directory in OUT_DIR");
		copy_dir_all(webapp_dist, &dest_path).expect("Failed to copy webapp dist files");
		generate_asset_manifest(&dest_path).expect("Failed to generate asset manifest");

		println!("cargo:warning=Webapp files copied to: {}", dest_path.display());
	} else {
		println!(
			"cargo:warning=No webapp/dist directory found. Run 'npm run build' in webapp/ directory first."
		);

		create_dir_all(&dest_path).expect("Failed to create webapp directory");

		let placeholder_html = r#"<!DOCTYPE html>
<html>
<head>
    <title>ReifyDB Admin</title>
    <style>
        body { font-family: system-ui; max-width: 800px; margin: 50px auto; padding: 20px; }
        .error { background: #fee; padding: 20px; border-radius: 5px; }
    </style>
</head>
<body>
    <h1>ReifyDB Admin Console</h1>
    <div class="error">
        <p>React app not found. Please build the webapp first.</p>
    </div>
</body>
</html>"#;

		fs::write(dest_path.join("index.html"), placeholder_html)
			.expect("Failed to write placeholder index.html");

		// Generate an empty asset manifest so the include! macro
		// doesn't fail
		generate_empty_asset_manifest(&dest_path).expect("Failed to generate empty asset manifest");
	}
}

fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> std::io::Result<()> {
	create_dir_all(&dst)?;
	for entry in fs::read_dir(src)? {
		let entry = entry?;
		let ty = entry.file_type()?;
		let src_path = entry.path();
		let file_name = entry.file_name();
		let dst_path = dst.as_ref().join(&file_name);

		if ty.is_dir() {
			copy_dir_all(&src_path, &dst_path)?;
		} else {
			fs::copy(&src_path, &dst_path)?;
		}
	}
	Ok(())
}

fn generate_asset_manifest(webapp_path: &Path) -> std::io::Result<()> {
	let manifest_path = webapp_path.join("asset_manifest.rs");
	let mut manifest = fs::File::create(&manifest_path)?;

	writeln!(manifest, "// Auto-generated asset manifest")?;
	writeln!(manifest, "pub const ASSETS: &[(&str, &[u8], &str)] = &[")?;

	// Collect all files recursively
	let mut assets = Vec::new();
	collect_assets(webapp_path, webapp_path, &mut assets)?;

	// Sort for consistent output
	assets.sort_by(|a, b| a.0.cmp(&b.0));

	// Write each asset entry
	for (path, full_path, mime_type) in assets {
		writeln!(
			manifest,
			"    (\"{}\", include_bytes!(\"{}\"), \"{}\"),",
			path,
			full_path.display(),
			mime_type
		)?;
	}

	writeln!(manifest, "];")?;
	Ok(())
}

fn collect_assets(
	base: &Path,
	dir: &Path,
	assets: &mut Vec<(String, std::path::PathBuf, String)>,
) -> std::io::Result<()> {
	for entry in fs::read_dir(dir)? {
		let entry = entry?;
		let path = entry.path();

		if path.is_dir() {
			collect_assets(base, &path, assets)?;
		} else if let Some(file_name) = path.file_name() {
			let file_name = file_name.to_string_lossy();

			// Skip the manifest file itself
			if file_name == "asset_manifest.rs" {
				continue;
			}

			// Get relative path from base
			let rel_path = path.strip_prefix(base).unwrap().to_string_lossy().replace('\\', "/");

			// Determine MIME type
			let mime_type = get_mime_type(&rel_path);

			assets.push((rel_path, path.clone(), mime_type.to_string()));
		}
	}
	Ok(())
}

fn get_mime_type(path: &str) -> &'static str {
	if path.ends_with(".html") {
		"text/html; charset=utf-8"
	} else if path.ends_with(".js") {
		"application/javascript"
	} else if path.ends_with(".css") {
		"text/css"
	} else if path.ends_with(".json") {
		"application/json"
	} else if path.ends_with(".png") {
		"image/png"
	} else if path.ends_with(".jpg") || path.ends_with(".jpeg") {
		"image/jpeg"
	} else if path.ends_with(".svg") {
		"image/svg+xml"
	} else if path.ends_with(".ico") {
		"image/x-icon"
	} else if path.ends_with(".woff") {
		"font/woff"
	} else if path.ends_with(".woff2") {
		"font/woff2"
	} else if path.ends_with(".ttf") {
		"font/ttf"
	} else if path.ends_with(".otf") {
		"font/otf"
	} else if path.ends_with(".eot") {
		"application/vnd.ms-fontobject"
	} else {
		"application/octet-stream"
	}
}

fn generate_empty_asset_manifest(webapp_path: &Path) -> std::io::Result<()> {
	let manifest_path = webapp_path.join("asset_manifest.rs");
	let mut manifest = fs::File::create(&manifest_path)?;

	writeln!(manifest, "// Auto-generated asset manifest (placeholder)")?;
	writeln!(manifest, "pub const ASSETS: &[(&str, &[u8], &str)] = &[")?;

	// Include just the placeholder index.html
	writeln!(manifest, "    (\"index.html\", include_bytes!(\"index.html\"), \"text/html; charset=utf-8\"),",)?;

	writeln!(manifest, "];")?;
	Ok(())
}
