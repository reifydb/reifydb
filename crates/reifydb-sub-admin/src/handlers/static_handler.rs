// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_network::HttpResponse;

use crate::assets;

pub fn serve_index() -> HttpResponse {
	// Try to get embedded index.html
	if let Some(file) = assets::get_embedded_file("index.html") {
		HttpResponse::ok()
			.with_header(
				"Content-Type".to_string(),
				file.mime_type.to_string(),
			)
			.with_body(file.content.to_vec())
	} else {
		// Fallback if no embedded file
		HttpResponse::ok().with_html(FALLBACK_HTML)
	}
}

const FALLBACK_HTML: &str = r#"<!DOCTYPE html>
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

pub fn serve_static(path: &str) -> HttpResponse {
	// Clean up the path - remove leading slash and handle both /assets/ and
	// direct paths
	let clean_path = path.strip_prefix('/').unwrap_or(path);

	// Try to serve embedded static file
	if let Some(file) = assets::get_embedded_file(clean_path) {
		HttpResponse::ok()
			.with_header(
				"Content-Type".to_string(),
				file.mime_type.to_string(),
			)
			.with_header(
				"Cache-Control".to_string(),
				"public, max-age=31536000".to_string(),
			)
			.with_body(file.content.to_vec())
	} else {
		HttpResponse::not_found().with_json(&format!(
			r#"{{"error":"Static file not found: {}"}}"#,
			clean_path
		))
	}
}
