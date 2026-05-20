// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	io,
	process::{Command, Output},
};

#[allow(dead_code)]
pub fn git_clone(url: &str, path: &str) -> io::Result<Output> {
	Command::new("git").args(["clone", url, path]).output()
}

#[allow(dead_code)]
pub fn git_checkout(path: &str, git_ref: &str) -> io::Result<Output> {
	Command::new("git").args(["-C", path, "checkout", git_ref]).output()
}

#[allow(dead_code)]
pub fn git_status(path: &str) -> io::Result<Output> {
	Command::new("git").args(["-C", path, "status", "--porcelain"]).output()
}
