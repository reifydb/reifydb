// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Regression guard for review #2 (store range "cap"), re-asserted across all three
// FlowTransaction variants. NativeStore::range wraps flow_txn.range(range, 1024);
// the 1024 is the storage pagination batch_size, NOT a row limit. With more than
// 1024 rows in range, range() must return every one in every variant - a native-
// side cap such as `.take(1024)` would make this fail.
