// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use error::OrderedFloatError;
pub use f32::OrderedF32;
pub use f64::OrderedF64;

mod error;
mod f32;
mod f64;
