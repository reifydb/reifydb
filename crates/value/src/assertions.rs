// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[macro_export]
macro_rules! reifydb_assertions {
	($($body:tt)*) => {
		#[cfg(reifydb_assertions)]
		{
			$($body)*
		}
	};
}
