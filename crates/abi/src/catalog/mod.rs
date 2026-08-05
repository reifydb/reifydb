// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The slice of catalog state an FFI extension can inspect. Deliberately narrow: enough to know the shape being
//! read from or written into, not the whole catalog hierarchy.

pub mod row_shape;
