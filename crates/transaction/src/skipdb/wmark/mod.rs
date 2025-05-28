// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! `Watermark` and `Closer` implementation for implementing transaction.
// #![allow(clippy::type_complexity)]
// #![deny(warnings, missing_docs)]
// #![cfg_attr(docsrs, feature(doc_cfg))]
// #![cfg_attr(docsrs, allow(unused_attributes))]

extern crate alloc;
extern crate std;

mod closer;

pub use closer::Closer;

mod watermark;

pub use watermark::{WaterMark, WaterMarkError};
