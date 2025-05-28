// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

//! Blazing fast ACID and MVCC in memory database.
//!
//! `skipdb` uses the same SSI (Serializable Snapshot Isolation) transaction model used in [`badger`](https://github.com/dgraph-io/badger).
// #![cfg_attr(docsrs, feature(doc_cfg))]
// #![cfg_attr(docsrs, allow(unused_attributes))]
// #![deny(missing_docs, warnings)]
// #![forbid(unsafe_code)]
// #![allow(clippy::type_complexity)]

use std::{borrow::Borrow, hash::BuildHasher, ops::RangeBounds, sync::Arc};

use crate::skipdb::txn::{BTreePwm, HashCm, Rtm, Tm, Wtm, error::TransactionError};

/// `OptimisticDb` implementation, which requires `K` implements both [`Hash`](core::hash::Hash) and [`Ord`].
///
/// If your `K` does not implement [`Hash`](core::hash::Hash), you can use [`SerializableDb`] instead.
pub mod optimistic;

/// `SerializableDb` implementation, which requires `K` implements [`Ord`] and [`CheapClone`](cheap_clone::CheapClone). If your `K` implements both [`Hash`](core::hash::Hash) and [`Ord`], you are recommended to use [`OptimisticDb`](crate::optimistic::OptimisticDb) instead.
pub mod serializable;

mod read;
pub use read::*;

pub use crate::skipdb::skipdbcore::{iter::*, range::*, rev_iter::*, types::Ref};

use crate::skipdb::skipdbcore::{AsSkipCore, Database, SkipCore};
