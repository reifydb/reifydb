// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	collections::HashSet,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_catalog::{bootstrap::load_catalog_cache, cache::CatalogCache};
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::version::{ComponentType, HasVersion, SystemVersion},
	util::ioc::IocContainer,
};
use reifydb_runtime::{shutdown::Shutdown, sync::mutex::Mutex};
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
use reifydb_value::Result;
use tokio::{runtime::Handle, task::JoinHandle};

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
		let catalog = ioc.resolve::<CatalogCache>()?;
		let eventbus = ioc.resolve::<EventBus>()?;
		let handle = ioc.resolve::<Handle>()?;

		Ok(Box::new(RaftSubsystem::new(
			self.config,
			multi_store,
			single_store,
			multi_tx,
			single_tx,
			catalog,
			eventbus,
			handle,
		)))
	}
}

pub struct RaftSubsystem {
	multi_tx: MultiTransaction,
	single_tx: SingleTransaction,
	handle: Handle,
	running: Arc<AtomicBool>,
	raft_handle: Mutex<Option<Raft>>,
	driver_join: Mutex<Option<JoinHandle<()>>>,
	transport_join: Mutex<Option<JoinHandle<()>>>,
}

impl RaftSubsystem {
	#[allow(clippy::too_many_arguments)]
	fn new(
		config: RaftConfig,
		multi_store: MultiStore,
		single_store: SingleStore,
		multi_tx: MultiTransaction,
		single_tx: SingleTransaction,
		catalog: CatalogCache,
		eventbus: EventBus,
		handle: Handle,
	) -> Self {
		let raft_state =
			Self::build_apply_state(multi_store, single_store, eventbus, &catalog, &multi_tx, &single_tx);
		let node = Self::build_seeded_node(&config, raft_state);
		let (transport, transport_join) = Self::start_transport(&handle, &config);
		let (raft, driver_join) = Self::spawn_driver(&handle, &config, node, transport);
		Self::wire_transactions(&multi_tx, &single_tx, &raft);
		Self::assemble(multi_tx, single_tx, handle, raft, driver_join, transport_join)
	}

	#[inline]
	fn build_apply_state(
		multi_store: MultiStore,
		single_store: SingleStore,
		eventbus: EventBus,
		catalog: &CatalogCache,
		multi_tx: &MultiTransaction,
		single_tx: &SingleTransaction,
	) -> Apply<MultiStore, SingleStore> {
		let catalog_for_cb = catalog.clone();
		let multi_for_cb = multi_tx.clone();
		let single_for_cb = single_tx.clone();
		let multi_for_ver = multi_tx.clone();

		Apply::with_callbacks(
			multi_store,
			single_store,
			eventbus,
			move || {
				if let Err(e) = load_catalog_cache(&multi_for_cb, &single_for_cb, &catalog_for_cb) {
					eprintln!("warning: catalog refresh failed: {e:?}");
				}
			},
			move |version| {
				multi_for_ver.advance_version_to(CommitVersion(version));
			},
		)
	}

	#[inline]
	fn build_seeded_node(config: &RaftConfig, raft_state: Apply<MultiStore, SingleStore>) -> Node {
		let peer_ids: HashSet<NodeId> = config.peers.iter().map(|p| p.node_id).collect();
		let opts = Options {
			heartbeat_interval: config.heartbeat_interval,
			election_timeout_range: config.election_timeout_range.clone(),
			max_append_entries: config.max_append_entries,
		};
		Node::new_seeded(
			config.node_id,
			peer_ids,
			Log::new(),
			Box::new(raft_state),
			opts,
			config.node_id as u64,
		)
	}

	#[inline]
	fn start_transport(handle: &Handle, config: &RaftConfig) -> (GrpcTransport, JoinHandle<()>) {
		handle.block_on(GrpcTransport::start(config.bind_addr, config.peers.clone()))
			.expect("failed to start raft gRPC transport")
	}

	#[inline]
	fn spawn_driver(
		handle: &Handle,
		config: &RaftConfig,
		node: Node,
		transport: GrpcTransport,
	) -> (Raft, JoinHandle<()>) {
		let driver_config = DriverConfig {
			tick_interval: config.tick_interval,
			recv_interval: config.recv_interval,
			proposal_channel_capacity: config.proposal_channel_capacity,
		};
		let (driver, raft) = RaftDriver::new(node, transport, driver_config);
		let driver_join = handle.spawn(driver.run());
		(raft, driver_join)
	}

	#[inline]
	fn wire_transactions(multi_tx: &MultiTransaction, single_tx: &SingleTransaction, raft: &Raft) {
		multi_tx.set_raft(raft.clone());
		single_tx.set_raft(raft.clone());
	}

	#[inline]
	fn assemble(
		multi_tx: MultiTransaction,
		single_tx: SingleTransaction,
		handle: Handle,
		raft: Raft,
		driver_join: JoinHandle<()>,
		transport_join: JoinHandle<()>,
	) -> Self {
		Self {
			multi_tx,
			single_tx,
			handle,
			running: Arc::new(AtomicBool::new(true)),
			raft_handle: Mutex::new(Some(raft)),
			driver_join: Mutex::new(Some(driver_join)),
			transport_join: Mutex::new(Some(transport_join)),
		}
	}

	pub fn raft(&self) -> Option<Raft> {
		self.raft_handle.lock().clone()
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

impl Shutdown for RaftSubsystem {
	fn shutdown(&self) {
		if self.running.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).is_err() {
			return;
		}

		self.multi_tx.clear_raft();
		self.single_tx.clear_raft();
		self.raft_handle.lock().take();

		let driver_join = self.driver_join.lock().take();
		if let Some(join) = driver_join {
			join.abort();
			let _ = self.handle.block_on(join);
		}

		let transport_join = self.transport_join.lock().take();
		if let Some(join) = transport_join {
			join.abort();
			let _ = self.handle.block_on(join);
		}
	}
}

impl Subsystem for RaftSubsystem {
	fn name(&self) -> &'static str {
		"Raft"
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
}
