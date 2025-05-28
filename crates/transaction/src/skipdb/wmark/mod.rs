// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

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
