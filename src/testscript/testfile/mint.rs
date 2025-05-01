// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/calder/rust-goldenfile (MIT License).
// Original MIT License Copyright (c) calder 2024.

//! Used to create testfiles.

use crate::testscript::testfile::differs::{binary_diff, text_diff, Differ};
use scopeguard::defer_on_unwind;
use std::env;
use std::fs;
use std::fs::File;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::thread;
use tempfile::TempDir;
use yansi::Paint;

/// A Mint creates testfiles.
///
/// When a Mint goes out of scope, it will do one of two things depending on the
/// value of the `UPDATE_TESTFILES` environment variable:
///
///   1. If `UPDATE_TESTFILES!=1`, it will check the new testfile
///      contents against their old contents, and panic if they differ.
///   2. If `UPDATE_TESTFILES=1`, it will replace the old testfile
///      contents with the newly written contents.
pub struct Mint {
    path: PathBuf,
    tempdir: TempDir,
    files: Vec<(PathBuf, Differ)>,
    create_empty: bool,
}

impl Mint {
    /// Create a new testfile Mint.
    fn new_internal<P: AsRef<Path>>(path: P, create_empty: bool) -> Self {
        let tempdir = TempDir::new().unwrap();
        let mint = Mint { path: path.as_ref().to_path_buf(), files: vec![], tempdir, create_empty };
        fs::create_dir_all(&mint.path).unwrap_or_else(|err| {
            panic!("Failed to create testfile directory {:?}: {:?}", mint.path, err)
        });
        mint
    }

    /// Create a new testfile Mint.
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self::new_internal(path, true)
    }

    /// Create a new testfile Mint. Testfiles will only be created when non-empty.
    pub fn new_nonempty<P: AsRef<Path>>(path: P) -> Self {
        Self::new_internal(path, false)
    }

    /// Create a new testfile using a differ inferred from the file extension.
    ///
    /// The returned File is a temporary file, not the testfile itself.
    pub fn new_testfile<P: AsRef<Path>>(&mut self, path: P) -> Result<File> {
        self.new_testfile_with_differ(&path, get_differ_for_path(&path))
    }

    /// Create a new testfile with the specified diff function.
    ///
    /// The returned File is a temporary file, not the testfile itself.
    pub fn new_testfile_with_differ<P: AsRef<Path>>(
        &mut self,
        path: P,
        differ: Differ,
    ) -> Result<File> {
        if path.as_ref().is_absolute() {
            return Err(Error::new(ErrorKind::InvalidInput, "Path must be relative."));
        }

        let abs_path = self.tempdir.path().to_path_buf().join(path.as_ref());
        if let Some(abs_parent) = abs_path.parent() {
            if abs_parent != self.tempdir.path() {
                fs::create_dir_all(abs_parent).unwrap_or_else(|err| {
                    panic!("Failed to create temporary subdirectory {:?}: {:?}", abs_parent, err)
                });
            }
        }
        let maybe_file = File::create(abs_path);
        if maybe_file.is_ok() {
            self.files.push((path.as_ref().to_path_buf(), differ));
        }
        maybe_file
    }

    /// Check new testfile contents against old, and panic if they differ.
    ///
    /// Called automatically when a Mint goes out of scope and
    /// `UPDATE_TESTFILES!=1`.
    pub fn check_testfiles(&self) {
        for (file, differ) in &self.files {
            let old = self.path.join(file);
            let new = self.tempdir.path().join(file);
            defer_on_unwind! {
                eprintln!("note: run with `UPDATE_TESTFILES=1` to update testfiles");
                eprintln!(
                    "{}: testfile changed: {}",
                    "error".bold().red(),
                    file.to_str().unwrap()
                );
            }
            differ(&old, &new);
        }
    }

    /// Overwrite old testfile contents with their new contents.
    ///
    /// Called automatically when a Mint goes out of scope and
    /// `UPDATE_TESTFILES=1`.
    pub fn update_testfiles(&self) {
        for (file, _) in &self.files {
            let old = self.path.join(file);
            let new = self.tempdir.path().join(file);

            let empty = File::open(&new).unwrap().metadata().unwrap().len() == 0;
            if self.create_empty || !empty {
                println!("Updating {:?}.", file.to_str().unwrap());
                fs::copy(&new, &old).unwrap_or_else(|err| {
                    panic!("Error copying {:?} to {:?}: {:?}", &new, &old, err)
                });
            } else if old.exists() {
                std::fs::remove_file(&old).unwrap();
            }
        }
    }
}

/// Get the diff function to use for a given file path.
pub fn get_differ_for_path<P: AsRef<Path>>(_path: P) -> Differ {
    match _path.as_ref().extension() {
        Some(os_str) => match os_str.to_str() {
            Some("bin") => Box::new(binary_diff),
            Some("exe") => Box::new(binary_diff),
            Some("gz") => Box::new(binary_diff),
            Some("tar") => Box::new(binary_diff),
            Some("zip") => Box::new(binary_diff),
            _ => Box::new(text_diff),
        },
        _ => Box::new(text_diff),
    }
}

impl Drop for Mint {
    /// Called when the mint goes out of scope to check or update testfiles.
    fn drop(&mut self) {
        if thread::panicking() {
            return;
        }
        // For backwards compatibility with 1.4 and below.
        let legacy_var = env::var("REGENERATE_TESTFILES");
        let update_var = env::var("UPDATE_TESTFILES");
        if (legacy_var.is_ok() && legacy_var.unwrap() == "1")
            || (update_var.is_ok() && update_var.unwrap() == "1")
        {
            self.update_testfiles();
        } else {
            self.check_testfiles();
        }
    }
}
