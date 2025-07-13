// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[derive(Debug)]
pub struct OrderedFloatError;

impl std::fmt::Display for OrderedFloatError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NaN cannot be used in ordered float")
    }
}

impl std::error::Error for OrderedFloatError {}
