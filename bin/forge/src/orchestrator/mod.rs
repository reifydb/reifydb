// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod procedures;
mod seed;

use std::{fs, path::PathBuf, sync::Arc, thread};

use axum::{
	Router,
	body::Body,
	extract::Request,
	http::StatusCode,
	response::{IntoResponse, Response},
	serve,
};
use reifydb::{WithSubsystem, server, sub_tracing::builder::TracingConfigurator};
use tokio::{net::TcpListener, runtime::Runtime};
use tracing::info;

use crate::{cli::Cli, shared::shape};

fn tracing_configuration(tracing: TracingConfigurator) -> TracingConfigurator {
	tracing.with_console(|console| console.color(true).stderr_for_errors(true)).with_filter("debug,reifydb=trace")
}

pub fn start(cli: &Cli) {
	let http_addr = cli.http_addr.clone();
	let grpc_addr = cli.grpc_addr.clone();
	let ws_addr = cli.ws_addr.clone();

	let mut db = server::memory()
		.with_grpc(|c| c.bind_addr(grpc_addr))
		.with_ws(|ws| ws.bind_addr(ws_addr))
		.with_tracing(tracing_configuration)
		.with_migrations(shape::migrations())
		.with_routines(|builder| {
			builder.register_procedure(Arc::new(procedures::RunPipelineProcedure::new()))
				.register_procedure(Arc::new(procedures::CancelRunProcedure::new()))
				.register_procedure(Arc::new(procedures::CompleteJobRunProcedure::new()))
				.register_procedure(Arc::new(procedures::ExecProcedure::new()))
		})
		.build()
		.unwrap();

	// Spawn Axum static file server for the SPA
	thread::spawn(move || {
		let rt = Runtime::new().unwrap();
		rt.block_on(async {
			let app = Router::new().fallback(serve_spa);
			let listener = TcpListener::bind(&http_addr).await.unwrap();
			info!("Forge HTTP server listening on {}", http_addr);
			serve(listener, app).await.unwrap();
		});
	});

	db.start().unwrap();
	seed::seed_default_pipeline(&db);
	db.await_signal().unwrap();
}

/// Simple static file server for the SPA. Serves files from `bin/forge/webapp/dist/`,
/// falling back to `index.html` for SPA routing.
async fn serve_spa(req: Request) -> Response {
	let dist_dir = PathBuf::from("bin/forge/webapp/dist");
	let path = req.uri().path().trim_start_matches('/');
	let file_path = dist_dir.join(if path.is_empty() {
		"index.html"
	} else {
		path
	});

	// Try to serve the requested file, fall back to index.html for SPA routes
	let target = if file_path.is_file() {
		file_path
	} else {
		dist_dir.join("index.html")
	};

	match fs::read(&target) {
		Ok(contents) => {
			let mime = match target.extension().and_then(|e| e.to_str()) {
				Some("html") => "text/html",
				Some("js") => "application/javascript",
				Some("css") => "text/css",
				Some("json") => "application/json",
				Some("svg") => "image/svg+xml",
				Some("png") => "image/png",
				Some("ico") => "image/x-icon",
				Some("woff") => "font/woff",
				Some("woff2") => "font/woff2",
				Some("ttf") => "font/ttf",
				_ => "application/octet-stream",
			};
			Response::builder().header("content-type", mime).body(Body::from(contents)).unwrap()
		}
		Err(_) => (StatusCode::NOT_FOUND, "Not Found").into_response(),
	}
}
