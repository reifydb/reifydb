// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// Submodule declarations
mod builder;
mod chain;
mod factory;
mod filter;
mod filtered;
mod interceptors;
mod namespace_def;
mod ringbuffer;
mod ringbuffer_def;
mod table;
mod table_def;
mod transaction;
mod view_def;

// Re-export chain
// Re-export builder
pub use builder::StandardInterceptorBuilder;
pub use chain::InterceptorChain;
// Re-export factory
pub use factory::{InterceptorFactory, StandardInterceptorFactory};
// Re-export filter
pub use filter::InterceptFilter;
// Re-export filtered interceptors
pub use filtered::*;
// Re-export interceptors container
pub use interceptors::Interceptors;
// Re-export namespace_def interceptors
pub use namespace_def::{
	ClosureNamespaceDefPostCreateInterceptor, ClosureNamespaceDefPostUpdateInterceptor,
	ClosureNamespaceDefPreDeleteInterceptor, ClosureNamespaceDefPreUpdateInterceptor,
	NamespaceDefPostCreateContext, NamespaceDefPostCreateInterceptor, NamespaceDefPostUpdateContext,
	NamespaceDefPostUpdateInterceptor, NamespaceDefPreDeleteContext, NamespaceDefPreDeleteInterceptor,
	NamespaceDefPreUpdateContext, NamespaceDefPreUpdateInterceptor, namespace_def_post_create,
	namespace_def_post_update, namespace_def_pre_delete, namespace_def_pre_update,
};
// Re-export ringbuffer interceptors
pub use ringbuffer::{
	ClosureRingBufferPostDeleteInterceptor, ClosureRingBufferPostInsertInterceptor,
	ClosureRingBufferPostUpdateInterceptor, ClosureRingBufferPreDeleteInterceptor,
	ClosureRingBufferPreInsertInterceptor, ClosureRingBufferPreUpdateInterceptor, RingBufferPostDeleteContext,
	RingBufferPostDeleteInterceptor, RingBufferPostInsertContext, RingBufferPostInsertInterceptor,
	RingBufferPostUpdateContext, RingBufferPostUpdateInterceptor, RingBufferPreDeleteContext,
	RingBufferPreDeleteInterceptor, RingBufferPreInsertContext, RingBufferPreInsertInterceptor,
	RingBufferPreUpdateContext, RingBufferPreUpdateInterceptor, ringbuffer_post_delete, ringbuffer_post_insert,
	ringbuffer_post_update, ringbuffer_pre_delete, ringbuffer_pre_insert, ringbuffer_pre_update,
};
// Re-export ringbuffer_def interceptors
pub use ringbuffer_def::{
	ClosureRingBufferDefPostCreateInterceptor, ClosureRingBufferDefPostUpdateInterceptor,
	ClosureRingBufferDefPreDeleteInterceptor, ClosureRingBufferDefPreUpdateInterceptor,
	RingBufferDefPostCreateContext, RingBufferDefPostCreateInterceptor, RingBufferDefPostUpdateContext,
	RingBufferDefPostUpdateInterceptor, RingBufferDefPreDeleteContext, RingBufferDefPreDeleteInterceptor,
	RingBufferDefPreUpdateContext, RingBufferDefPreUpdateInterceptor, ringbuffer_def_post_create,
	ringbuffer_def_post_update, ringbuffer_def_pre_delete, ringbuffer_def_pre_update,
};
// Re-export table interceptors
pub use table::{
	ClosureTablePostDeleteInterceptor, ClosureTablePostInsertInterceptor, ClosureTablePostUpdateInterceptor,
	ClosureTablePreDeleteInterceptor, ClosureTablePreInsertInterceptor, ClosureTablePreUpdateInterceptor,
	TablePostDeleteContext, TablePostDeleteInterceptor, TablePostInsertContext, TablePostInsertInterceptor,
	TablePostUpdateContext, TablePostUpdateInterceptor, TablePreDeleteContext, TablePreDeleteInterceptor,
	TablePreInsertContext, TablePreInsertInterceptor, TablePreUpdateContext, TablePreUpdateInterceptor,
	table_post_delete, table_post_insert, table_post_update, table_pre_delete, table_pre_insert, table_pre_update,
};
// Re-export table_def interceptors
pub use table_def::{
	ClosureTableDefPostCreateInterceptor, ClosureTableDefPostUpdateInterceptor,
	ClosureTableDefPreDeleteInterceptor, ClosureTableDefPreUpdateInterceptor, TableDefPostCreateContext,
	TableDefPostCreateInterceptor, TableDefPostUpdateContext, TableDefPostUpdateInterceptor,
	TableDefPreDeleteContext, TableDefPreDeleteInterceptor, TableDefPreUpdateContext, TableDefPreUpdateInterceptor,
	table_def_post_create, table_def_post_update, table_def_pre_delete, table_def_pre_update,
};
// Re-export transaction interceptors
pub use transaction::{
	ClosurePostCommitInterceptor, ClosurePreCommitInterceptor, PostCommitContext, PostCommitInterceptor,
	PreCommitContext, PreCommitInterceptor, post_commit, pre_commit,
};
// Re-export view_def interceptors
pub use view_def::{
	ClosureViewDefPostCreateInterceptor, ClosureViewDefPostUpdateInterceptor, ClosureViewDefPreDeleteInterceptor,
	ClosureViewDefPreUpdateInterceptor, ViewDefPostCreateContext, ViewDefPostCreateInterceptor,
	ViewDefPostUpdateContext, ViewDefPostUpdateInterceptor, ViewDefPreDeleteContext, ViewDefPreDeleteInterceptor,
	ViewDefPreUpdateContext, ViewDefPreUpdateInterceptor, view_def_post_create, view_def_post_update,
	view_def_pre_delete, view_def_pre_update,
};

// Type alias for convenience
pub type Chain<I> = InterceptorChain<I>;

/// Trait for accessing interceptor chains from transaction types
pub trait WithInterceptors {
	/// Access table pre-insert interceptor chain
	fn table_pre_insert_interceptors(&mut self) -> &mut Chain<dyn TablePreInsertInterceptor + Send + Sync>;

	/// Access table post-insert interceptor chain
	fn table_post_insert_interceptors(&mut self) -> &mut Chain<dyn TablePostInsertInterceptor + Send + Sync>;

	/// Access table pre-update interceptor chain
	fn table_pre_update_interceptors(&mut self) -> &mut Chain<dyn TablePreUpdateInterceptor + Send + Sync>;

	/// Access table post-update interceptor chain
	fn table_post_update_interceptors(&mut self) -> &mut Chain<dyn TablePostUpdateInterceptor + Send + Sync>;

	/// Access table pre-delete interceptor chain
	fn table_pre_delete_interceptors(&mut self) -> &mut Chain<dyn TablePreDeleteInterceptor + Send + Sync>;

	/// Access table post-delete interceptor chain
	fn table_post_delete_interceptors(&mut self) -> &mut Chain<dyn TablePostDeleteInterceptor + Send + Sync>;

	/// Access ring buffer pre-insert interceptor chain
	fn ringbuffer_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreInsertInterceptor + Send + Sync>;

	/// Access ring buffer post-insert interceptor chain
	fn ringbuffer_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostInsertInterceptor + Send + Sync>;

	/// Access ring buffer pre-update interceptor chain
	fn ringbuffer_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreUpdateInterceptor + Send + Sync>;

	/// Access ring buffer post-update interceptor chain
	fn ringbuffer_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostUpdateInterceptor + Send + Sync>;

	/// Access ring buffer pre-delete interceptor chain
	fn ringbuffer_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreDeleteInterceptor + Send + Sync>;

	/// Access ring buffer post-delete interceptor chain
	fn ringbuffer_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostDeleteInterceptor + Send + Sync>;

	/// Access pre-commit interceptor chain
	fn pre_commit_interceptors(&mut self) -> &mut Chain<dyn PreCommitInterceptor + Send + Sync>;

	/// Access post-commit interceptor chain
	fn post_commit_interceptors(&mut self) -> &mut Chain<dyn PostCommitInterceptor + Send + Sync>;

	/// Access namespace post-create interceptor chain
	fn namespace_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPostCreateInterceptor + Send + Sync>;

	/// Access namespace pre-update interceptor chain
	fn namespace_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPreUpdateInterceptor + Send + Sync>;

	/// Access namespace post-update interceptor chain
	fn namespace_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPostUpdateInterceptor + Send + Sync>;

	/// Access namespace pre-delete interceptor chain
	fn namespace_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPreDeleteInterceptor + Send + Sync>;

	/// Access table definition post-create interceptor chain
	fn table_def_post_create_interceptors(&mut self)
	-> &mut Chain<dyn TableDefPostCreateInterceptor + Send + Sync>;

	/// Access table definition pre-update interceptor chain
	fn table_def_pre_update_interceptors(&mut self) -> &mut Chain<dyn TableDefPreUpdateInterceptor + Send + Sync>;

	/// Access table definition post-update interceptor chain
	fn table_def_post_update_interceptors(&mut self)
	-> &mut Chain<dyn TableDefPostUpdateInterceptor + Send + Sync>;

	/// Access table definition pre-delete interceptor chain
	fn table_def_pre_delete_interceptors(&mut self) -> &mut Chain<dyn TableDefPreDeleteInterceptor + Send + Sync>;

	/// Access view post-create interceptor chain
	fn view_def_post_create_interceptors(&mut self) -> &mut Chain<dyn ViewDefPostCreateInterceptor + Send + Sync>;

	/// Access view pre-update interceptor chain
	fn view_def_pre_update_interceptors(&mut self) -> &mut Chain<dyn ViewDefPreUpdateInterceptor + Send + Sync>;

	/// Access view post-update interceptor chain
	fn view_def_post_update_interceptors(&mut self) -> &mut Chain<dyn ViewDefPostUpdateInterceptor + Send + Sync>;

	/// Access view pre-delete interceptor chain
	fn view_def_pre_delete_interceptors(&mut self) -> &mut Chain<dyn ViewDefPreDeleteInterceptor + Send + Sync>;

	/// Access ring buffer definition post-create interceptor chain
	fn ringbuffer_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPostCreateInterceptor + Send + Sync>;

	/// Access ring buffer definition pre-update interceptor chain
	fn ringbuffer_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPreUpdateInterceptor + Send + Sync>;

	/// Access ring buffer definition post-update interceptor chain
	fn ringbuffer_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPostUpdateInterceptor + Send + Sync>;

	/// Access ring buffer definition pre-delete interceptor chain
	fn ringbuffer_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPreDeleteInterceptor + Send + Sync>;
}
