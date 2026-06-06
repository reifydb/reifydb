// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::fs;

use reifydb::{Database, ExportOptions, ShapeKind, SqliteConfig, embedded};

use crate::{LocalAction, LocalCommand, LocalExportArgs, LocalImportArgs};

pub fn handle(command: LocalCommand) -> Result<(), String> {
	match command.action {
		LocalAction::Export(args) => export(args),
		LocalAction::Import(args) => import(args),
	}
}

fn open(db_path: &str) -> Result<Database, String> {
	embedded::sqlite(SqliteConfig::new(db_path))
		.build()
		.map_err(|e| format!("failed to open database '{}': {}", db_path, e))
}

fn export(args: LocalExportArgs) -> Result<(), String> {
	let options = build_options(&args)?;
	let mut db = open(&args.db)?;
	let result = db.export(&options).map_err(|e| format!("export failed: {}", e));
	let _ = db.stop();
	let script = result?;

	match &args.out {
		Some(path) => fs::write(path, &script).map_err(|e| format!("failed to write '{}': {}", path, e))?,
		None => print!("{}", script),
	}
	Ok(())
}

fn import(args: LocalImportArgs) -> Result<(), String> {
	let mut db = open(&args.db)?;
	let result = db.import_from_file(&args.file).map_err(|e| format!("import failed: {}", e));
	let _ = db.stop();
	result?;
	eprintln!("imported '{}' into '{}'", args.file, args.db);
	Ok(())
}

fn build_options(args: &LocalExportArgs) -> Result<ExportOptions, String> {
	if args.schema_only && args.data_only {
		return Err("--schema-only and --data-only are mutually exclusive".to_string());
	}

	let mut options = ExportOptions::all();

	if !args.shape.is_empty() {
		for shape in &args.shape {
			let (namespace, name) = shape
				.split_once("::")
				.ok_or_else(|| format!("invalid --shape '{}', expected namespace::name", shape))?;
			options = options.shape(namespace, name);
		}
	} else if !args.kind.is_empty() {
		for kind in &args.kind {
			options = options.kind(parse_kind(kind)?);
		}
	} else if !args.namespace.is_empty() {
		for namespace in &args.namespace {
			options = options.namespace(namespace);
		}
	}

	if args.schema_only {
		options = options.schema_only();
	}
	if args.data_only {
		options = options.data_only();
	}
	if let Some(size) = args.batch_size {
		options = options.batch_size(size);
	}
	if args.if_not_exists {
		options = options.if_not_exists(true);
	}

	Ok(options)
}

fn parse_kind(kind: &str) -> Result<ShapeKind, String> {
	match kind.to_ascii_lowercase().as_str() {
		"table" => Ok(ShapeKind::Table),
		"ringbuffer" => Ok(ShapeKind::RingBuffer),
		"series" => Ok(ShapeKind::Series),
		"dictionary" => Ok(ShapeKind::Dictionary),
		"enum" => Ok(ShapeKind::Enum),
		other => Err(format!("unknown --kind '{}', expected table|ringbuffer|series|dictionary|enum", other)),
	}
}
