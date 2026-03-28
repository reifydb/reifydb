// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{io, process::Command};

#[allow(dead_code)]
pub fn git_clone(url: &str, path: &str) -> io::Result<std::process::Output> {
	Command::new("git").args(["clone", url, path]).output()
}

#[allow(dead_code)]
pub fn git_checkout(path: &str, git_ref: &str) -> io::Result<std::process::Output> {
	Command::new("git").args(["-C", path, "checkout", git_ref]).output()
}

#[allow(dead_code)]
pub fn git_status(path: &str) -> io::Result<std::process::Output> {
	Command::new("git").args(["-C", path, "status", "--porcelain"]).output()
}
