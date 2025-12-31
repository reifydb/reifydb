// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod aggregate;
pub mod assign;
pub mod compile;
pub mod conditional;
pub mod declare;
pub mod dictionary_scan;
pub mod environment;
pub mod extend;
pub mod filter;
pub mod generator;
pub mod index_scan;
pub mod inline;
pub mod join;
pub mod map;
pub mod ringbuffer_scan;
pub mod row_lookup;
pub mod scalarize;
pub mod sort;
pub mod table_scan;
pub mod take;
pub mod top_k;
pub mod variable;
pub mod view_scan;
pub mod vtable_scan;
