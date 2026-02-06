// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// Submodule declarations
pub mod builder;
pub mod chain;
pub mod factory;
pub mod filter;
pub mod filtered;
pub mod interceptors;
pub mod namespace_def;
pub mod ringbuffer;
pub mod ringbuffer_def;
pub mod table;
pub mod table_def;
pub mod transaction;
pub mod view;
pub mod view_def;

// Re-import types for use in WithInterceptors trait
use chain::InterceptorChain;
use namespace_def::{
	NamespaceDefPostCreateInterceptor, NamespaceDefPostUpdateInterceptor, NamespaceDefPreDeleteInterceptor,
	NamespaceDefPreUpdateInterceptor,
};
use ringbuffer::{
	RingBufferPostDeleteInterceptor, RingBufferPostInsertInterceptor, RingBufferPostUpdateInterceptor,
	RingBufferPreDeleteInterceptor, RingBufferPreInsertInterceptor, RingBufferPreUpdateInterceptor,
};
use ringbuffer_def::{
	RingBufferDefPostCreateInterceptor, RingBufferDefPostUpdateInterceptor, RingBufferDefPreDeleteInterceptor,
	RingBufferDefPreUpdateInterceptor,
};
use table::{
	TablePostDeleteInterceptor, TablePostInsertInterceptor, TablePostUpdateInterceptor, TablePreDeleteInterceptor,
	TablePreInsertInterceptor, TablePreUpdateInterceptor,
};
use table_def::{
	TableDefPostCreateInterceptor, TableDefPostUpdateInterceptor, TableDefPreDeleteInterceptor,
	TableDefPreUpdateInterceptor,
};
use transaction::{PostCommitInterceptor, PreCommitInterceptor};
use view::{
	ViewPostDeleteInterceptor, ViewPostInsertInterceptor, ViewPostUpdateInterceptor, ViewPreDeleteInterceptor,
	ViewPreInsertInterceptor, ViewPreUpdateInterceptor,
};
use view_def::{
	ViewDefPostCreateInterceptor, ViewDefPostUpdateInterceptor, ViewDefPreDeleteInterceptor,
	ViewDefPreUpdateInterceptor,
};

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

	/// Access view pre-insert interceptor chain
	fn view_pre_insert_interceptors(&mut self) -> &mut Chain<dyn ViewPreInsertInterceptor + Send + Sync>;

	/// Access view post-insert interceptor chain
	fn view_post_insert_interceptors(&mut self) -> &mut Chain<dyn ViewPostInsertInterceptor + Send + Sync>;

	/// Access view pre-update interceptor chain
	fn view_pre_update_interceptors(&mut self) -> &mut Chain<dyn ViewPreUpdateInterceptor + Send + Sync>;

	/// Access view post-update interceptor chain
	fn view_post_update_interceptors(&mut self) -> &mut Chain<dyn ViewPostUpdateInterceptor + Send + Sync>;

	/// Access view pre-delete interceptor chain
	fn view_pre_delete_interceptors(&mut self) -> &mut Chain<dyn ViewPreDeleteInterceptor + Send + Sync>;

	/// Access view post-delete interceptor chain
	fn view_post_delete_interceptors(&mut self) -> &mut Chain<dyn ViewPostDeleteInterceptor + Send + Sync>;

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
