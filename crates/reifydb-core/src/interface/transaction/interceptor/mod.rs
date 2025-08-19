// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod table;
mod transaction;

pub use table::TableInterceptor;
pub use transaction::TransactionInterceptor;

/// Macro to generate interceptor execution methods
#[macro_export]
macro_rules! impl_interceptor_method {
	(
		$method_name:ident,
		$field:ident,
		$interceptor_trait:ident,
		$context_type:ident,
		($($param:ident: $type:ty),*)
	) => {
		fn $method_name(
			&mut self,
			$($param: $type),*
		) -> crate::Result<()> {
			if self.interceptors.$field.is_empty() {
				return Ok(());
			}
			// We need to use unsafe here to work around the borrow checker
			// This is safe because:
			// 1. We know the interceptor chain won't outlive this function
			//    call
			// 2. The execution is synchronous and single-threaded
			// 3. We're only borrowing different parts of self
			unsafe {
				let chain_ptr: *mut InterceptorChain<
					T,
					dyn $interceptor_trait<T>,
				> = &mut self.interceptors.$field as *mut _;
				let ctx = $context_type::new(self, $($param),*);
				(*chain_ptr).execute(ctx)?
			}
			Ok(())
		}
	};
}
