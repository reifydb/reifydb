// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::vtable::user::UserVTableColumn;
use reifydb_core::{interface::catalog::id::NamespaceId, value::column::columns::Columns};
use reifydb_value::value::datetime::DateTime;

pub trait MetricsSource: Send + Sync + 'static {
	fn namespace(&self) -> NamespaceId;

	fn columns(&self) -> Vec<UserVTableColumn>;

	fn collect(&self, now: DateTime) -> Columns;
}
