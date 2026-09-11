// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_dst)]
compile_error!(
	"reifydb-node cannot be built under REIFYDB_DST: napi runs sync methods on the JS thread and \
	 polls async ones on its own runtime, which the Rc-backed dst executor cannot survive"
);

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use napi::{
	Error as NapiError, Result,
	bindgen_prelude::{Buffer, Either},
};
use napi_derive::napi;
use reifydb::{
	Database, Frame as CoreFrame, IdentityId, Migration, MigrationSource, Result as ReifyResult, WithSubsystem,
	auth::service::AuthResponse, core::interface::catalog::id::SubscriptionId, embedded,
	subscription::batch::BatchId,
};
use reifydb_codec::json::{to::convert_frames, wire_type::WireValueType};
use reifydb_sub_server::wire::{WireParams, WireValue};
use reifydb_value::{params::Params, value::uuid::Uuid7};
use serde_json::{Value as JsonValue, from_value, json, to_string as json_to_string, to_value};
use uuid::Uuid;

pub mod subscription;

use crate::subscription::{Subscriptions, sink::NodePush};

#[napi]
#[derive(Clone)]
pub struct ReifydbNode {
	db: Arc<Database>,
	subscriptions: Arc<Subscriptions>,
}

#[napi(object)]
pub struct ParamValue {
	pub r#type: JsonValue,
	pub value: String,
}

#[napi(object)]
pub struct Column {
	pub name: String,
	pub r#type: JsonValue,
	pub payload: Vec<String>,
}

#[napi(object)]
pub struct Frame {
	pub columns: Vec<Column>,
}

#[napi(object)]
pub struct BatchMember {
	pub index: u32,
	pub subscription_id: String,
}

#[napi(object)]
pub struct BatchSubscribed {
	pub batch_id: String,
	pub members: Vec<BatchMember>,
}

#[napi(object)]
pub struct ClosedMember {
	pub batch_id: String,
	pub subscription_id: String,
}

/// What one [`ReifydbNode::tick`] produced.
///
/// `envelopes` are byte-identical to the binary frames the WebSocket transport sends, so the same
/// client decoder reads them. The close notifications are values rather than encoded frames: they
/// carry ids and no offsets, so there is nothing a second encoder could get subtly wrong.
#[napi(object)]
pub struct SubscriptionTick {
	pub envelopes: Vec<Buffer>,
	pub closed: Vec<String>,
	pub batch_member_closed: Vec<ClosedMember>,
}

fn frames_to_napi(frames: &[CoreFrame]) -> Vec<Frame> {
	convert_frames(frames)
		.into_iter()
		.map(|frame| Frame {
			columns: frame
				.columns
				.into_iter()
				.map(|column| Column {
					name: column.name,
					r#type: to_value(&column.r#type)
						.expect("value type is always representable as JSON"),
					payload: column.payload,
				})
				.collect(),
		})
		.collect()
}

type ParamsInput = Either<Vec<ParamValue>, HashMap<String, ParamValue>>;

fn parse_params(params: Option<ParamsInput>) -> Result<Params> {
	let wire = match params {
		None => return Ok(Params::None),
		Some(Either::A(items)) => WireParams::Positional(
			items.into_iter()
				.enumerate()
				.map(|(index, value)| to_wire_value(&format!("${}", index + 1), value))
				.collect::<Result<_>>()?,
		),
		Some(Either::B(map)) => WireParams::Named(
			map.into_iter()
				.map(|(name, value)| -> Result<_> {
					let wire = to_wire_value(&format!("${name}"), value)?;
					Ok((name, wire))
				})
				.collect::<Result<_>>()?,
		),
	};
	wire.into_params().map_err(NapiError::from_reason)
}

fn to_wire_value(parameter: &str, param: ParamValue) -> Result<WireValue> {
	let r#type = from_value::<WireValueType>(param.r#type)
		.map_err(|e| NapiError::from_reason(format!("parameter {parameter}: unknown type: {e}")))?;
	Ok(WireValue {
		r#type,
		value: param.value,
	})
}

impl ReifydbNode {
	pub fn new(migrations: impl Into<MigrationSource>) -> ReifyResult<Self> {
		let db = embedded::memory().with_flow(|flow| flow).with_migrations(migrations).build()?;
		Self::wrap(db)
	}

	/// Binds a built database to a subscription transport. Every entry point goes through here so
	/// none of them can hand back a node whose subscriptions were never wired up.
	fn wrap(db: Database) -> ReifyResult<Self> {
		let subscriptions = Subscriptions::new(&db)?;
		Ok(Self {
			db: Arc::new(db),
			subscriptions: Arc::new(subscriptions),
		})
	}
}

#[napi(object)]
pub struct MigrationEntry {
	pub dir: Option<String>,
	pub name: Option<String>,
	pub statements: Option<Vec<String>>,
	pub rollback: Option<Vec<String>>,
}

fn migration_source(entry: MigrationEntry) -> MigrationSource {
	match entry.dir {
		Some(dir) => MigrationSource::Directory(PathBuf::from(dir)),
		None => {
			let name = entry.name.unwrap_or_default();
			let statements = entry.statements.unwrap_or_default();
			let migration = match entry.rollback {
				Some(rollback) => Migration::with_rollback(name, statements, rollback),
				None => Migration::new(name, statements),
			};
			MigrationSource::List(vec![migration])
		}
	}
}

#[napi(js_name = "openWithMigrations")]
pub fn open_with_migrations(entries: Vec<MigrationEntry>) -> Result<ReifydbNode> {
	let mut builder = embedded::memory().with_flow(|flow| flow);
	if !entries.is_empty() {
		let sources = entries.into_iter().map(migration_source).collect();
		builder = builder.with_migrations(MigrationSource::Multiple(sources));
	}
	let db = builder.build().map_err(|e| NapiError::from_reason(format!("{e:?}")))?;
	ReifydbNode::wrap(db).map_err(|e| NapiError::from_reason(format!("{e:?}")))
}

#[napi]
impl ReifydbNode {
	#[napi(js_name = "adminRoot")]
	pub async fn admin_root(&self, rql: String, params: Option<ParamsInput>) -> Result<Vec<Frame>> {
		let node = self.clone();
		offload(move || node.admin_root_now(rql, params)).await
	}

	#[napi(js_name = "commandRoot")]
	pub async fn command_root(&self, rql: String, params: Option<ParamsInput>) -> Result<Vec<Frame>> {
		let node = self.clone();
		offload(move || node.command_root_now(rql, params)).await
	}

	#[napi(js_name = "queryRoot")]
	pub async fn query_root(&self, rql: String, params: Option<ParamsInput>) -> Result<Vec<Frame>> {
		let node = self.clone();
		offload(move || node.query_root_now(rql, params)).await
	}

	#[napi(js_name = "adminAs")]
	pub async fn admin_as(&self, identity: String, rql: String, params: Option<ParamsInput>) -> Result<Vec<Frame>> {
		let node = self.clone();
		offload(move || node.admin_as_now(identity, rql, params)).await
	}

	#[napi(js_name = "commandAs")]
	pub async fn command_as(
		&self,
		identity: String,
		rql: String,
		params: Option<ParamsInput>,
	) -> Result<Vec<Frame>> {
		let node = self.clone();
		offload(move || node.command_as_now(identity, rql, params)).await
	}

	#[napi(js_name = "queryAs")]
	pub async fn query_as(&self, identity: String, rql: String, params: Option<ParamsInput>) -> Result<Vec<Frame>> {
		let node = self.clone();
		offload(move || node.query_as_now(identity, rql, params)).await
	}

	/// `rql` is the whole `CREATE SUBSCRIPTION ... AS { .. }` statement, exactly as a client sends
	/// it over a socket. Returns the subscription id; changes arrive from [`Self::tick`].
	#[napi(js_name = "subscribeRoot")]
	pub async fn subscribe_root(&self, rql: String, params: Option<ParamsInput>) -> Result<String> {
		self.subscribe_with(IdentityId::root(), rql, params).await
	}

	#[napi(js_name = "subscribeAs")]
	pub async fn subscribe_as(&self, identity: String, rql: String, params: Option<ParamsInput>) -> Result<String> {
		let identity = parse_identity(&identity)?;
		self.subscribe_with(identity, rql, params).await
	}

	#[napi(js_name = "batchSubscribeRoot")]
	pub async fn batch_subscribe_root(&self, queries: Vec<String>) -> Result<BatchSubscribed> {
		self.batch_subscribe_with(IdentityId::root(), queries).await
	}

	#[napi(js_name = "batchSubscribeAs")]
	pub async fn batch_subscribe_as(&self, identity: String, queries: Vec<String>) -> Result<BatchSubscribed> {
		let identity = parse_identity(&identity)?;
		self.batch_subscribe_with(identity, queries).await
	}

	#[napi(js_name = "batchUnsubscribe")]
	pub async fn batch_unsubscribe(&self, batch_id: String) -> Result<()> {
		self.batch_unsubscribe_with(parse_batch_id(&batch_id)?).await
	}

	#[napi(js_name = "caughtUp")]
	pub async fn caught_up(&self) -> Result<SubscriptionTick> {
		let node = self.clone();
		// A tick holds `Buffer`s, so only the pushes may cross the thread boundary.
		Ok(to_tick(offload(move || node.caught_up_pushes()).await?))
	}

	#[napi]
	pub async fn authenticate(&self, method: String, credentials: HashMap<String, String>) -> Result<String> {
		let node = self.clone();
		offload(move || node.authenticate_now(method, credentials)).await
	}
}

#[napi]
impl ReifydbNode {
	#[napi]
	pub fn unsubscribe(&self, subscription_id: String) -> Result<()> {
		let subscription_id = parse_subscription_id(&subscription_id)?;
		self.subscriptions.unsubscribe(subscription_id).map_err(|e| NapiError::from_reason(format!("{e:?}")))
	}

	#[napi]
	pub fn tick(&self) -> Result<SubscriptionTick> {
		Ok(to_tick(self.subscriptions.poll()))
	}
}

impl ReifydbNode {
	fn admin_root_now(&self, rql: String, params: Option<ParamsInput>) -> Result<Vec<Frame>> {
		let params = parse_params(params)?;
		render(self.db.admin_as_root(&rql, params))
	}

	fn command_root_now(&self, rql: String, params: Option<ParamsInput>) -> Result<Vec<Frame>> {
		let params = parse_params(params)?;
		render(self.db.command_as_root(&rql, params))
	}

	fn query_root_now(&self, rql: String, params: Option<ParamsInput>) -> Result<Vec<Frame>> {
		let params = parse_params(params)?;
		render(self.db.query_as_root(&rql, params))
	}

	fn admin_as_now(&self, identity: String, rql: String, params: Option<ParamsInput>) -> Result<Vec<Frame>> {
		let identity = parse_identity(&identity)?;
		let params = parse_params(params)?;
		render(self.db.admin_as(identity, &rql, params))
	}

	fn command_as_now(&self, identity: String, rql: String, params: Option<ParamsInput>) -> Result<Vec<Frame>> {
		let identity = parse_identity(&identity)?;
		let params = parse_params(params)?;
		render(self.db.command_as(identity, &rql, params))
	}

	fn query_as_now(&self, identity: String, rql: String, params: Option<ParamsInput>) -> Result<Vec<Frame>> {
		let identity = parse_identity(&identity)?;
		let params = parse_params(params)?;
		render(self.db.query_as(identity, &rql, params))
	}

	fn caught_up_pushes(&self) -> Result<Vec<NodePush>> {
		self.subscriptions.caught_up(&self.db).map_err(|e| NapiError::from_reason(format!("{e:?}")))
	}

	fn authenticate_now(&self, method: String, credentials: HashMap<String, String>) -> Result<String> {
		self.db.auth_service()
			.authenticate(&method, credentials)
			.map(|response| auth_response_to_json(&response))
			.map_err(|e| NapiError::from_reason(format!("{e:?}")))
	}
}

impl ReifydbNode {
	async fn subscribe_with(
		&self,
		identity: IdentityId,
		rql: String,
		params: Option<ParamsInput>,
	) -> Result<String> {
		let params = parse_params(params)?;
		self.subscriptions
			.subscribe(identity, rql, params)
			.await
			.map(|id| id.to_string())
			.map_err(|e| NapiError::from_reason(format!("{e:?}")))
	}

	async fn batch_unsubscribe_with(&self, batch_id: BatchId) -> Result<()> {
		self.subscriptions.batch_unsubscribe(batch_id).await;
		Ok(())
	}

	async fn batch_subscribe_with(&self, identity: IdentityId, queries: Vec<String>) -> Result<BatchSubscribed> {
		let ack = self
			.subscriptions
			.batch_subscribe(identity, &queries)
			.await
			.map_err(|e| NapiError::from_reason(format!("{e:?}")))?;
		Ok(BatchSubscribed {
			batch_id: ack.batch_id.to_string(),
			members: ack
				.members
				.into_iter()
				.map(|m| BatchMember {
					index: m.index as u32,
					subscription_id: m.subscription_id.to_string(),
				})
				.collect(),
		})
	}
}

async fn offload<T, F>(work: F) -> Result<T>
where
	T: Send + 'static,
	F: FnOnce() -> Result<T> + Send + 'static,
{
	tokio::task::spawn_blocking(work).await.expect("blocking task panicked")
}

fn to_tick(pushes: Vec<NodePush>) -> SubscriptionTick {
	let mut tick = SubscriptionTick {
		envelopes: Vec::new(),
		closed: Vec::new(),
		batch_member_closed: Vec::new(),
	};
	for push in pushes {
		match push {
			NodePush::Change {
				envelope,
			}
			| NodePush::BatchChange {
				envelope,
			} => tick.envelopes.push(envelope.into()),
			NodePush::Closed {
				subscription_id,
			} => tick.closed.push(subscription_id.to_string()),
			NodePush::BatchMemberClosed {
				batch_id,
				subscription_id,
			} => tick.batch_member_closed.push(ClosedMember {
				batch_id: batch_id.to_string(),
				subscription_id: subscription_id.to_string(),
			}),
		}
	}
	tick
}

fn render(result: ReifyResult<Vec<CoreFrame>>) -> Result<Vec<Frame>> {
	result.map(|frames| frames_to_napi(&frames)).map_err(|e| NapiError::from_reason(format!("{e:?}")))
}

fn parse_batch_id(raw: &str) -> Result<BatchId> {
	raw.parse::<BatchId>().map_err(|_| NapiError::from_reason(format!("invalid batch id: {raw}")))
}

fn parse_subscription_id(raw: &str) -> Result<SubscriptionId> {
	raw.parse::<u64>()
		.map(SubscriptionId)
		.map_err(|_| NapiError::from_reason(format!("invalid subscription id: {raw}")))
}

fn parse_identity(raw: &str) -> Result<IdentityId> {
	let uuid = Uuid::parse_str(raw).map_err(|e| NapiError::from_reason(format!("invalid identity: {e}")))?;
	Ok(IdentityId::new(Uuid7::from(uuid)))
}

fn auth_response_to_json(response: &AuthResponse) -> String {
	let json = match response {
		AuthResponse::Authenticated {
			identity,
			token,
		} => json!({"status": "authenticated", "identity": identity.to_string(), "token": token}),
		AuthResponse::Challenge {
			challenge_id,
			payload,
		} => json!({"status": "challenge", "challengeId": challenge_id, "payload": payload}),
		AuthResponse::Failed {
			reason,
		} => json!({"status": "failed", "reason": reason}),
	};
	json_to_string(&json).expect("auth response fields are all plain strings")
}
