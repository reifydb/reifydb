// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	any::Any,
	collections::HashSet,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_catalog::{
	bootstrap::load_materialized_catalog, materialized::MaterializedCatalog, schema::RowSchemaRegistry,
};
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::version::{ComponentType, HasVersion, SystemVersion},
	util::ioc::IocContainer,
};
use reifydb_runtime::SharedRuntime;
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_sub_api::subsystem::{HealthStatus, Subsystem, SubsystemFactory};
use reifydb_sub_raft::{
	config::RaftConfig,
	driver::{DriverConfig, Raft, RaftDriver},
	grpc::GrpcTransport,
	log::Log,
	node::{Node, NodeId, Options},
	state::apply::Apply,
};
use reifydb_transaction::{multi::transaction::MultiTransaction, single::SingleTransaction};
use reifydb_type::Result;
use tokio::task::JoinHandle;

pub struct RaftSubsystemFactory {
	config: RaftConfig,
}

impl RaftSubsystemFactory {
	pub fn new(config: RaftConfig) -> Self {
		Self {
			config,
		}
	}
}

impl SubsystemFactory for RaftSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let multi_store = ioc.resolve::<MultiStore>()?;
		let single_store = ioc.resolve::<SingleStore>()?;
		let multi_tx = ioc.resolve::<MultiTransaction>()?;
		let single_tx = ioc.resolve::<SingleTransaction>()?;
		let catalog = ioc.resolve::<MaterializedCatalog>()?;
		let schema = ioc.resolve::<RowSchemaRegistry>()?;
		let eventbus = ioc.resolve::<EventBus>()?;
		let runtime = ioc.resolve::<SharedRuntime>()?;

		Ok(Box::new(RaftSubsystem::new(
			self.config,
			multi_store,
			single_store,
			multi_tx,
			single_tx,
			catalog,
			schema,
			eventbus,
			runtime,
		)))
	}
}

pub struct RaftSubsystem {
	config: RaftConfig,
	multi_store: MultiStore,
	single_store: SingleStore,
	multi_tx: MultiTransaction,
	single_tx: SingleTransaction,
	catalog: MaterializedCatalog,
	schema: RowSchemaRegistry,
	eventbus: EventBus,
	runtime: SharedRuntime,
	running: Arc<AtomicBool>,
	raft_handle: Option<Raft>,
	driver_join: Option<JoinHandle<()>>,
	transport_join: Option<JoinHandle<()>>,
}

impl RaftSubsystem {
	fn new(
		config: RaftConfig,
		multi_store: MultiStore,
		single_store: SingleStore,
		multi_tx: MultiTransaction,
		single_tx: SingleTransaction,
		catalog: MaterializedCatalog,
		schema: RowSchemaRegistry,
		eventbus: EventBus,
		runtime: SharedRuntime,
	) -> Self {
		Self {
			config,
			multi_store,
			single_store,
			multi_tx,
			single_tx,
			catalog,
			schema,
			eventbus,
			runtime,
			running: Arc::new(AtomicBool::new(false)),
			raft_handle: None,
			driver_join: None,
			transport_join: None,
		}
	}

	pub fn raft(&self) -> Option<&Raft> {
		self.raft_handle.as_ref()
	}
}

impl HasVersion for RaftSubsystem {
	fn version(&self) -> SystemVersion {
		SystemVersion {
			name: "sub-raft".to_string(),
			version: env!("CARGO_PKG_VERSION").to_string(),
			description: "Raft consensus subsystem for distributed replication".to_string(),
			r#type: ComponentType::Subsystem,
		}
	}
}

impl Subsystem for RaftSubsystem {
	fn name(&self) -> &'static str {
		"Raft"
	}

	fn start(&mut self) -> Result<()> {
		if self.running.load(Ordering::SeqCst) {
			return Ok(());
		}

		let catalog_for_cb = self.catalog.clone();
		let schema_for_cb = self.schema.clone();
		let multi_for_cb = self.multi_tx.clone();
		let single_for_cb = self.single_tx.clone();
		let multi_for_ver = self.multi_tx.clone();

		let raft_state = Apply::with_callbacks(
			self.multi_store.clone(),
			self.single_store.clone(),
			self.eventbus.clone(),
			move || {
				if let Err(e) =
					load_materialized_catalog(&multi_for_cb, &single_for_cb, &catalog_for_cb)
				{
					eprintln!("warning: catalog refresh failed: {e:?}");
				}
				if let Err(e) = schema_for_cb.reload_from_storage() {
					eprintln!("warning: schema refresh failed: {e:?}");
				}
			},
			move |version| {
				multi_for_ver.advance_version_to(CommitVersion(version));
			},
		);

		let peer_ids: HashSet<NodeId> = self.config.peers.iter().map(|p| p.node_id).collect();
		let opts = Options {
			heartbeat_interval: self.config.heartbeat_interval,
			election_timeout_range: self.config.election_timeout_range.clone(),
			max_append_entries: self.config.max_append_entries,
		};
		let node = Node::new_seeded(
			self.config.node_id,
			peer_ids,
			Log::new(),
			Box::new(raft_state),
			opts,
			self.config.node_id as u64,
		);

		let (transport, transport_join) = self
			.runtime
			.block_on(GrpcTransport::start(
				self.config.node_id,
				self.config.bind_addr,
				self.config.peers.clone(),
			))
			.expect("failed to start raft gRPC transport");

		let driver_config = DriverConfig {
			tick_interval: self.config.tick_interval,
			recv_interval: self.config.recv_interval,
			proposal_channel_capacity: self.config.proposal_channel_capacity,
		};
		let (driver, handle) = RaftDriver::new(node, transport, driver_config);
		let driver_join = self.runtime.spawn(driver.run());

		self.multi_tx.set_raft(handle.clone());
		self.single_tx.set_raft(handle.clone());

		self.raft_handle = Some(handle);
		self.driver_join = Some(driver_join);
		self.transport_join = Some(transport_join);
		self.running.store(true, Ordering::SeqCst);

		Ok(())
	}

	fn shutdown(&mut self) -> Result<()> {
		if !self.running.load(Ordering::SeqCst) {
			return Ok(());
		}

		self.multi_tx.clear_raft();
		self.single_tx.clear_raft();
		self.raft_handle.take();

		if let Some(join) = self.driver_join.take() {
			join.abort();
			let _ = self.runtime.block_on(join);
		}

		if let Some(join) = self.transport_join.take() {
			join.abort();
			let _ = self.runtime.block_on(join);
		}

		self.running.store(false, Ordering::SeqCst);
		Ok(())
	}

	fn is_running(&self) -> bool {
		self.running.load(Ordering::SeqCst)
	}

	fn health_status(&self) -> HealthStatus {
		if self.running.load(Ordering::SeqCst) {
			HealthStatus::Healthy
		} else {
			HealthStatus::Unknown
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}
