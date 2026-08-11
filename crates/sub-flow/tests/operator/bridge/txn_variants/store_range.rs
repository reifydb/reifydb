// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The store range re-asserted across all three FlowTransaction variants: the 1024 handed to
// flow_txn.range is the storage pagination batch_size, not a row limit, so more than 1024 rows in
// range must all come back in every variant.
