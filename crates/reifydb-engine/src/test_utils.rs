// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogStore, MaterializedCatalog,
	schema::SchemaToCreate,
	table::{TableColumnToCreate, TableToCreate},
};
use reifydb_core::{Type, hook::Hooks, interceptor::Interceptors};
use reifydb_storage::memory::Memory;
use reifydb_transaction::{
	mvcc::transaction::serializable::Serializable, svl::SingleVersionLock,
};

use crate::{
	EngineTransaction, StandardCommandTransaction,
	transaction::StandardCdcTransaction,
};

pub fn create_test_command_transaction() -> StandardCommandTransaction<
	EngineTransaction<
		Serializable<Memory, SingleVersionLock<Memory>>,
		SingleVersionLock<Memory>,
		StandardCdcTransaction<Memory>,
	>,
> {
	let memory = Memory::new();
	let hooks = Hooks::new();
	let unversioned = SingleVersionLock::new(memory.clone(), hooks.clone());
	let cdc = StandardCdcTransaction::new(memory.clone());
	StandardCommandTransaction::new(
		Serializable::new(memory, unversioned.clone(), hooks.clone())
			.begin_command()
			.unwrap(),
		unversioned,
		cdc,
		hooks,
		MaterializedCatalog::new(),
		Interceptors::new(),
	)
}

pub fn create_test_command_transaction_with_internal_schema()
-> StandardCommandTransaction<
	EngineTransaction<
		Serializable<Memory, SingleVersionLock<Memory>>,
		SingleVersionLock<Memory>,
		StandardCdcTransaction<Memory>,
	>,
> {
	let memory = Memory::new();
	let hooks = Hooks::new();
	let unversioned = SingleVersionLock::new(memory.clone(), hooks.clone());
	let cdc = StandardCdcTransaction::new(memory.clone());
	let mut result = StandardCommandTransaction::new(
		Serializable::new(memory, unversioned.clone(), hooks.clone())
			.begin_command()
			.unwrap(),
		unversioned,
		cdc,
		hooks,
		MaterializedCatalog::new(),
		Interceptors::new(),
	);

	let schema = CatalogStore::create_schema(
		&mut result,
		SchemaToCreate {
			schema_fragment: None,
			name: "reifydb".to_string(),
		},
	)
	.unwrap();

	CatalogStore::create_table(
		&mut result,
		TableToCreate {
			fragment: None,
			schema: schema.id,
			table: "flows".to_string(),
			columns: vec![
				TableColumnToCreate {
					name: "id".to_string(),
					ty: Type::Int8,
					policies: vec![],
					auto_increment: true,
					fragment: None,
				},
				TableColumnToCreate {
					name: "data".to_string(),
					ty: Type::Blob,
					policies: vec![],
					auto_increment: false,
					fragment: None,
				},
			],
		},
	)
	.unwrap();

	result
}
