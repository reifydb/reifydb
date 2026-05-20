// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::VTable;

use super::VTable;
























pub trait VirtualTableFactory: Send + Sync + 'static {



	fn create_boxed(&self) -> Box<dyn BaseVTable + Send + Sync>;




	fn definition(&self) -> Arc<VTable>;
}
