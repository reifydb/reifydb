// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::VTable;

use super::VTable;
























pub trait VirtualTableFactory: Send + Sync + 'static {



	fn create_boxed(&self) -> Box<dyn BaseVTable + Send + Sync>;




	fn definition(&self) -> Arc<VTable>;
}
