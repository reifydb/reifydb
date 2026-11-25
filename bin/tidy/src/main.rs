// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod rule;
mod walk;

use std::{
	collections::BTreeMap,
	env,
	error::Error,
	fs,
	path::{Path, PathBuf},
	process::ExitCode,
	sync::{
		atomic::{AtomicUsize, Ordering},
		mpsc::{self, Receiver, Sender},
	},
	thread,
};

type Checked = (usize, Result<Vec<String>, String>);

struct Root(PathBuf);

struct Totals {
	violations: usize,
	files: usize,
}

fn main() -> ExitCode {
	match run() {
		Ok(code) => code,
		Err(error) => panic!("tidy failed: {error}"),
	}
}

fn run() -> Result<ExitCode, Box<dyn Error>> {
	let root = Root(env::current_dir()?);
	let Some(paths) = run_paths(&root.0)? else {
		eprintln!("usage: reifydb-tidy [changed]");
		return Ok(ExitCode::from(2));
	};
	let totals = run_check(&root, &paths)?;
	Ok(run_summary(&totals))
}

fn run_paths(root: &Path) -> Result<Option<Vec<PathBuf>>, Box<dyn Error>> {
	match env::args().nth(1).as_deref() {
		None => Ok(Some(walk::all(root)?)),
		Some("changed") => Ok(Some(walk::changed(root)?)),
		Some(_) => Ok(None),
	}
}

fn run_check(root: &Root, paths: &[PathBuf]) -> Result<Totals, Box<dyn Error>> {
	let workers = thread::available_parallelism()?.get();
	let next = AtomicUsize::new(0);
	let (sender, receiver) = mpsc::channel();
	thread::scope(|scope| {
		for _ in 0..workers {
			let sender = sender.clone();
			let next = &next;
			scope.spawn(move || run_check_worker(root, paths, next, sender));
		}
		drop(sender);
		run_check_print(root, paths, receiver)
	})
}

fn run_check_worker(root: &Root, paths: &[PathBuf], next: &AtomicUsize, sender: Sender<Checked>) {
	loop {
		let index = next.fetch_add(1, Ordering::Relaxed);
		let Some(path) = paths.get(index) else {
			return;
		};
		if sender.send((index, run_check_worker_file(root, path))).is_err() {
			return;
		}
	}
}

fn run_check_worker_file(root: &Root, path: &Path) -> Result<Vec<String>, String> {
	let shown = root.relative(path);
	let text = fs::read_to_string(path).map_err(|error| format!("{shown}: {error}"))?;
	let breaches = rule::check(&text).map_err(|error| format!("{shown}: {error}"))?;
	Ok(breaches.iter().map(|breach| format!("  {} :{} {}", breach.rule, breach.line, breach.function)).collect())
}

fn run_check_print(root: &Root, paths: &[PathBuf], receiver: Receiver<Checked>) -> Result<Totals, Box<dyn Error>> {
	let mut totals = Totals {
		violations: 0,
		files: 0,
	};
	let mut waiting = BTreeMap::new();
	let mut due = 0;
	for (index, lines) in receiver {
		waiting.insert(index, lines?);
		while let Some(lines) = waiting.remove(&due) {
			run_check_print_file(&root.relative(&paths[due]), &lines, &mut totals);
			due += 1;
		}
	}
	Ok(totals)
}

fn run_check_print_file(shown: &str, lines: &[String], totals: &mut Totals) {
	if lines.is_empty() {
		return;
	}
	println!("{shown}");
	for line in lines {
		println!("{line}");
	}
	totals.violations += lines.len();
	totals.files += 1;
}

impl Root {
	fn relative(&self, path: &Path) -> String {
		path.strip_prefix(&self.0).unwrap_or(path).display().to_string()
	}
}

fn run_summary(totals: &Totals) -> ExitCode {
	if totals.violations == 0 {
		println!("tidy: ok");
		return ExitCode::SUCCESS;
	}
	println!("tidy: {} violations in {} files", totals.violations, totals.files);
	ExitCode::FAILURE
}
