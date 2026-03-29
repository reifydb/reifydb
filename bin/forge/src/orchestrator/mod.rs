// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod procedures;
mod seed;

use std::path::PathBuf;

use axum::{
	body::Body,
	extract::Request,
	response::{IntoResponse, Response},
};
use reifydb::{
	WithSubsystem, server, sub_server_grpc::factory::GrpcConfig, sub_server_ws::factory::WsConfig,
	sub_tracing::builder::TracingBuilder,
};

use crate::{cli::Cli, shared::shape};

fn tracing_configuration(tracing: TracingBuilder) -> TracingBuilder {
	tracing.with_console(|console| console.color(true).stderr_for_errors(true)).with_filter("debug,reifydb=trace")
}

pub fn start(cli: &Cli) {
	let http_addr = cli.http_addr.clone();

	let mut db = server::memory()
		.with_grpc(GrpcConfig::default().bind_addr(&cli.grpc_addr))
		.with_ws(WsConfig::default().bind_addr(&cli.ws_addr))
		.with_tracing(tracing_configuration)
		.with_migrations(shape::migrations())
		.with_procedures(|builder| {
			builder.with_procedure("forge::run_pipeline", || procedures::RunPipelineProcedure)
				.with_procedure("forge::cancel_run", || procedures::CancelRunProcedure)
				.with_procedure("forge::complete_job_run", || procedures::CompleteJobRunProcedure)
				.with_procedure("forge::exec", || procedures::ExecProcedure)
		})
		.build()
		.unwrap();

	// Spawn Axum static file server for the SPA
	std::thread::spawn(move || {
		let rt = tokio::runtime::Runtime::new().unwrap();
		rt.block_on(async {
			let app = axum::Router::new().fallback(serve_spa);
			let listener = tokio::net::TcpListener::bind(&http_addr).await.unwrap();
			tracing::info!("Forge HTTP server listening on {}", http_addr);
			axum::serve(listener, app).await.unwrap();
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

	match std::fs::read(&target) {
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
		Err(_) => (axum::http::StatusCode::NOT_FOUND, "Not Found").into_response(),
	}
}
