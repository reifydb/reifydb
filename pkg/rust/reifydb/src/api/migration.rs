// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	fs,
	path::{Path, PathBuf},
};

use reifydb_core::internal_error;
use reifydb_type::Result;

#[derive(Debug, Clone)]
pub struct Migration {
	pub name: String,
	pub body: String,
	pub rollback_body: Option<String>,
}

impl Migration {
	pub fn new(name: impl Into<String>, statements: Vec<impl Into<String>>) -> Self {
		let body = statements.into_iter().map(|s| s.into()).collect::<Vec<_>>().join(";\n");
		Self {
			name: name.into(),
			body,
			rollback_body: None,
		}
	}

	pub fn with_rollback(
		name: impl Into<String>,
		statements: Vec<impl Into<String>>,
		rollback_statements: Vec<impl Into<String>>,
	) -> Self {
		let body = statements.into_iter().map(|s| s.into()).collect::<Vec<_>>().join(";\n");
		let rollback_body = rollback_statements.into_iter().map(|s| s.into()).collect::<Vec<_>>().join(";\n");
		Self {
			name: name.into(),
			body,
			rollback_body: Some(rollback_body),
		}
	}
}

#[derive(Debug, Clone)]
pub enum MigrationStatement {
	Wrapped {
		name: String,
		body: String,
		rollback_body: Option<String>,
	},
	Raw(String),
}

impl From<Migration> for MigrationStatement {
	fn from(m: Migration) -> Self {
		Self::Wrapped {
			name: m.name,
			body: m.body,
			rollback_body: m.rollback_body,
		}
	}
}

#[derive(Debug, Clone)]
pub enum MigrationSource {
	List(Vec<Migration>),
	Directory(PathBuf),
	Multiple(Vec<MigrationSource>),
}

impl MigrationSource {
	pub fn resolve(self) -> Result<Vec<MigrationStatement>> {
		match self {
			Self::List(migrations) => Ok(migrations.into_iter().map(MigrationStatement::from).collect()),
			Self::Directory(path) => load_directory(&path),
			Self::Multiple(sources) => {
				let mut out = Vec::new();
				for s in sources {
					out.extend(s.resolve()?);
				}
				Ok(out)
			}
		}
	}
}

impl From<Vec<Migration>> for MigrationSource {
	fn from(migrations: Vec<Migration>) -> Self {
		Self::List(migrations)
	}
}

impl From<Migration> for MigrationSource {
	fn from(migration: Migration) -> Self {
		Self::List(vec![migration])
	}
}

impl From<PathBuf> for MigrationSource {
	fn from(path: PathBuf) -> Self {
		Self::Directory(path)
	}
}

impl From<&Path> for MigrationSource {
	fn from(path: &Path) -> Self {
		Self::Directory(path.to_path_buf())
	}
}

impl From<&str> for MigrationSource {
	fn from(path: &str) -> Self {
		Self::Directory(PathBuf::from(path))
	}
}

impl From<String> for MigrationSource {
	fn from(path: String) -> Self {
		Self::Directory(PathBuf::from(path))
	}
}

const RQL_EXTENSION: &str = ".rql";

fn load_directory(path: &Path) -> Result<Vec<MigrationStatement>> {
	let entries = fs::read_dir(path)
		.map_err(|e| internal_error!("failed to read migration directory `{}`: {}", path.display(), e))?;

	let mut files: Vec<(String, PathBuf)> = Vec::new();

	for entry in entries {
		let entry = entry.map_err(|e| internal_error!("failed to enumerate `{}`: {}", path.display(), e))?;

		let metadata = entry
			.metadata()
			.map_err(|e| internal_error!("failed to stat `{}`: {}", entry.path().display(), e))?;
		if !metadata.is_file() {
			continue;
		}

		let file_name = entry.file_name();
		let file_name = match file_name.to_str() {
			Some(s) => s.to_string(),
			None => {
				return Err(internal_error!(
					"migration file `{}` has a non-UTF-8 name",
					entry.path().display()
				));
			}
		};

		if !file_name.ends_with(RQL_EXTENSION) || file_name == RQL_EXTENSION {
			continue;
		}

		files.push((file_name, entry.path()));
	}

	files.sort_by(|a, b| a.0.cmp(&b.0));

	let mut out = Vec::with_capacity(files.len());
	for (_, file_path) in &files {
		let content = fs::read_to_string(file_path)
			.map_err(|e| internal_error!("failed to read `{}`: {}", file_path.display(), e))?;
		out.push(MigrationStatement::Raw(content));
	}

	Ok(out)
}

#[cfg(test)]
mod tests {
	use std::{
		env::temp_dir,
		fs,
		path::PathBuf,
		process::id,
		time::{SystemTime, UNIX_EPOCH},
	};

	use super::*;

	fn unique_dir(label: &str) -> PathBuf {
		let mut p = temp_dir();
		let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
		p.push(format!("reifydb_migsrc_{}_{}_{}", label, id(), nanos));
		fs::create_dir_all(&p).unwrap();
		p
	}

	fn raw(stmt: &MigrationStatement) -> &str {
		match stmt {
			MigrationStatement::Raw(s) => s.as_str(),
			MigrationStatement::Wrapped {
				..
			} => panic!("expected Raw, got Wrapped"),
		}
	}

	#[test]
	fn from_vec_makes_list() {
		let v = vec![Migration::new("a", vec!["x"])];
		match MigrationSource::from(v) {
			MigrationSource::List(items) => assert_eq!(items.len(), 1),
			_ => panic!("expected List"),
		}
	}

	#[test]
	fn from_path_makes_directory() {
		let p = PathBuf::from("/tmp/whatever");
		match MigrationSource::from(p.clone()) {
			MigrationSource::Directory(got) => assert_eq!(got, p),
			_ => panic!("expected Directory"),
		}
	}

	#[test]
	fn list_resolve_yields_wrapped_statements() {
		let m = Migration::new("a", vec!["x"]);
		let resolved = MigrationSource::List(vec![m]).resolve().unwrap();
		assert_eq!(resolved.len(), 1);
		match &resolved[0] {
			MigrationStatement::Wrapped {
				name,
				body,
				rollback_body,
			} => {
				assert_eq!(name, "a");
				assert_eq!(body, "x");
				assert!(rollback_body.is_none());
			}
			_ => panic!("expected Wrapped"),
		}
	}

	#[test]
	fn directory_empty_returns_empty() {
		let dir = unique_dir("empty");
		let r = MigrationSource::Directory(dir.clone()).resolve().unwrap();
		assert!(r.is_empty());
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn directory_loads_files_in_alphabetical_order_as_raw() {
		let dir = unique_dir("order");
		fs::write(dir.join("002_b.rql"), "STMT_B").unwrap();
		fs::write(dir.join("001_a.rql"), "STMT_A").unwrap();
		fs::write(dir.join("003_c.rql"), "STMT_C").unwrap();

		let r = MigrationSource::Directory(dir.clone()).resolve().unwrap();
		assert_eq!(r.len(), 3);
		assert_eq!(raw(&r[0]), "STMT_A");
		assert_eq!(raw(&r[1]), "STMT_B");
		assert_eq!(raw(&r[2]), "STMT_C");
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn directory_loads_file_content_verbatim_no_parsing() {
		let dir = unique_dir("verbatim");
		let content = "CREATE MIGRATION '001_x' {\n    CREATE NAMESPACE app\n} ROLLBACK {\n    DROP NAMESPACE app\n};\n";
		fs::write(dir.join("001_x.rql"), content).unwrap();

		let r = MigrationSource::Directory(dir.clone()).resolve().unwrap();
		assert_eq!(r.len(), 1);
		assert_eq!(raw(&r[0]), content);
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn directory_missing_returns_error() {
		let path = PathBuf::from("/this/path/should/not/exist/for/reifydb_migsrc_test");
		let err = MigrationSource::Directory(path).resolve().unwrap_err();
		let msg = format!("{}", err);
		assert!(msg.contains("failed to read migration directory"), "got: {msg}");
	}

	#[test]
	fn directory_ignores_non_rql_and_subdirs_and_bare_extension() {
		let dir = unique_dir("ignore");
		fs::write(dir.join("001_x.rql"), "REAL").unwrap();
		fs::write(dir.join("readme.md"), "ignored").unwrap();
		fs::write(dir.join("script.sql"), "also ignored").unwrap();
		fs::write(dir.join(".rql"), "bare extension, must be skipped").unwrap();
		fs::create_dir(dir.join("subdir")).unwrap();
		fs::write(dir.join("subdir/inner.rql"), "should not be loaded").unwrap();

		let r = MigrationSource::Directory(dir.clone()).resolve().unwrap();
		assert_eq!(r.len(), 1);
		assert_eq!(raw(&r[0]), "REAL");
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn directory_uppercase_extension_is_not_matched() {
		let dir = unique_dir("upper");
		fs::write(dir.join("001_x.RQL"), "UPPER").unwrap();
		fs::write(dir.join("002_y.rql"), "LOWER").unwrap();
		let r = MigrationSource::Directory(dir.clone()).resolve().unwrap();
		assert_eq!(r.len(), 1);
		assert_eq!(raw(&r[0]), "LOWER");
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn multiple_concatenates_in_order_preserving_variant() {
		let dir = unique_dir("multi");
		fs::write(dir.join("dir_a.rql"), "DIR_A").unwrap();
		fs::write(dir.join("dir_b.rql"), "DIR_B").unwrap();

		let src = MigrationSource::Multiple(vec![
			MigrationSource::List(vec![Migration::new("list_first", vec!["L1"])]),
			MigrationSource::Directory(dir.clone()),
			MigrationSource::List(vec![Migration::new("list_last", vec!["L2"])]),
		]);
		let r = src.resolve().unwrap();
		assert_eq!(r.len(), 4);
		match &r[0] {
			MigrationStatement::Wrapped {
				name,
				..
			} => assert_eq!(name, "list_first"),
			_ => panic!("expected Wrapped"),
		}
		assert_eq!(raw(&r[1]), "DIR_A");
		assert_eq!(raw(&r[2]), "DIR_B");
		match &r[3] {
			MigrationStatement::Wrapped {
				name,
				..
			} => assert_eq!(name, "list_last"),
			_ => panic!("expected Wrapped"),
		}
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn multiple_propagates_inner_directory_failure() {
		let bad = PathBuf::from("/this/path/does/not/exist/reifydb_inner_test");
		let src = MigrationSource::Multiple(vec![
			MigrationSource::List(vec![Migration::new("list_first", vec!["L1"])]),
			MigrationSource::Directory(bad),
		]);
		let err = src.resolve().unwrap_err();
		let msg = format!("{}", err);
		assert!(msg.contains("failed to read migration directory"), "got: {msg}");
	}

	#[test]
	fn directory_path_pointing_to_file_errors() {
		let dir = unique_dir("isfile");
		let file = dir.join("not_a_dir.rql");
		fs::write(&file, "x").unwrap();
		let err = MigrationSource::Directory(file).resolve().unwrap_err();
		let msg = format!("{}", err);
		assert!(msg.contains("failed to read migration directory"), "got: {msg}");
		fs::remove_dir_all(dir).ok();
	}

	#[test]
	fn from_migration_yields_wrapped() {
		let m = Migration::with_rollback("n", vec!["b"], vec!["rb"]);
		let stmt: MigrationStatement = m.into();
		match stmt {
			MigrationStatement::Wrapped {
				name,
				body,
				rollback_body,
			} => {
				assert_eq!(name, "n");
				assert_eq!(body, "b");
				assert_eq!(rollback_body.as_deref(), Some("rb"));
			}
			_ => panic!("expected Wrapped"),
		}
	}
}
