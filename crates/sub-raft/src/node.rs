// Copyright (c) 2026 ReifyDB
// SPDX-License-Identifier: AGPL-3.0-or-later

use std::{
	cmp::{max, min},
	collections::{HashMap, HashSet},
	mem,
	ops::Range,
};

use rand::{RngExt as _, SeedableRng, rng, rngs::SmallRng};
use reifydb_runtime::reifydb_assertions;

use crate::{
	log::{Entry, Index, Log},
	message::{Command, Envelope, Message},
	state::State,
};

pub type NodeId = u8;

pub type Term = u64;

pub type Ticks = u8;

pub const HEARTBEAT_INTERVAL: Ticks = 4;

pub const ELECTION_TIMEOUT_RANGE: Range<Ticks> = 10..20;

pub const MAX_APPEND_ENTRIES: usize = 100;

#[derive(Clone, Debug, PartialEq)]
pub struct Options {
	pub heartbeat_interval: Ticks,
	pub election_timeout_range: Range<Ticks>,
	pub max_append_entries: usize,
}

impl Default for Options {
	fn default() -> Self {
		Self {
			heartbeat_interval: HEARTBEAT_INTERVAL,
			election_timeout_range: ELECTION_TIMEOUT_RANGE,
			max_append_entries: MAX_APPEND_ENTRIES,
		}
	}
}

pub enum Node {
	Candidate(RawNode<Candidate>),
	Follower(RawNode<Follower>),
	Leader(RawNode<Leader>),
}

impl Node {
	pub fn new(id: NodeId, peers: HashSet<NodeId>, log: Log, state: Box<dyn State>, opts: Options) -> Self {
		let rng = SmallRng::from_rng(&mut rng());
		Self::new_with_rng(id, peers, log, state, opts, rng)
	}

	pub fn new_seeded(
		id: NodeId,
		peers: HashSet<NodeId>,
		log: Log,
		state: Box<dyn State>,
		opts: Options,
		seed: u64,
	) -> Self {
		let rng = SmallRng::seed_from_u64(seed);
		Self::new_with_rng(id, peers, log, state, opts, rng)
	}

	fn new_with_rng(
		id: NodeId,
		peers: HashSet<NodeId>,
		log: Log,
		state: Box<dyn State>,
		opts: Options,
		rng: SmallRng,
	) -> Self {
		let node = RawNode::new(id, peers, log, state, opts, rng);
		if node.cluster_size() == 1 {
			return node.into_candidate().into_leader().into();
		}
		node.into()
	}

	pub fn id(&self) -> NodeId {
		match self {
			Self::Candidate(n) => n.id,
			Self::Follower(n) => n.id,
			Self::Leader(n) => n.id,
		}
	}

	pub fn term(&self) -> Term {
		match self {
			Self::Candidate(n) => n.term(),
			Self::Follower(n) => n.term(),
			Self::Leader(n) => n.term(),
		}
	}

	pub fn role(&self) -> &'static str {
		match self {
			Self::Candidate(_) => "candidate",
			Self::Follower(_) => "follower",
			Self::Leader(_) => "leader",
		}
	}

	pub fn step(self, msg: Envelope) -> Self {
		assert_eq!(msg.to, self.id(), "message to other node: {msg:?}");
		match self {
			Self::Candidate(n) => n.step(msg),
			Self::Follower(n) => n.step(msg),
			Self::Leader(n) => n.step(msg),
		}
	}

	pub fn tick(self) -> Self {
		match self {
			Self::Candidate(n) => n.tick(),
			Self::Follower(n) => n.tick(),
			Self::Leader(n) => n.tick(),
		}
	}

	pub fn drain_outbox(&mut self) -> Vec<Envelope> {
		match self {
			Self::Candidate(n) => mem::take(&mut n.outbox),
			Self::Follower(n) => mem::take(&mut n.outbox),
			Self::Leader(n) => mem::take(&mut n.outbox),
		}
	}

	pub fn log(&self) -> &Log {
		match self {
			Self::Candidate(n) => &n.log,
			Self::Follower(n) => &n.log,
			Self::Leader(n) => &n.log,
		}
	}

	pub fn applied_index(&self) -> Index {
		match self {
			Self::Candidate(n) => n.state.get_applied_index(),
			Self::Follower(n) => n.state.get_applied_index(),
			Self::Leader(n) => n.state.get_applied_index(),
		}
	}

	pub fn propose(&mut self, command: Command) -> Index {
		match self {
			Self::Leader(n) => {
				let index = n.propose(command);
				if n.cluster_size() == 1 {
					n.maybe_commit_and_apply();
				}
				index
			}
			_ => panic!("can only propose on leader"),
		}
	}

	pub fn progress(&self) -> Option<&HashMap<NodeId, Progress>> {
		match self {
			Self::Leader(n) => Some(&n.role.progress),
			_ => None,
		}
	}

	pub fn leader(&self) -> Option<NodeId> {
		match self {
			Self::Follower(n) => n.role.leader,
			_ => None,
		}
	}

	pub fn state(&self) -> &dyn State {
		match self {
			Self::Candidate(n) => n.state.as_ref(),
			Self::Follower(n) => n.state.as_ref(),
			Self::Leader(n) => n.state.as_ref(),
		}
	}

	pub fn get_commit_index(&self) -> (Index, Term) {
		self.log().get_commit_index()
	}

	pub fn get_term_vote(&self) -> (Term, Option<NodeId>) {
		self.log().get_term_vote()
	}

	pub fn peers(&self) -> &HashSet<NodeId> {
		match self {
			Self::Candidate(n) => &n.peers,
			Self::Follower(n) => &n.peers,
			Self::Leader(n) => &n.peers,
		}
	}

	pub fn options(&self) -> &Options {
		match self {
			Self::Candidate(n) => &n.opts,
			Self::Follower(n) => &n.opts,
			Self::Leader(n) => &n.opts,
		}
	}

	pub fn scan_log(&self) -> Vec<Entry> {
		let (last_index, _) = self.log().get_last_index();
		if last_index == 0 {
			return Vec::new();
		}
		self.log().scan(1, last_index)
	}

	pub fn dismantle(self) -> (NodeId, HashSet<NodeId>, Log, Box<dyn State>, Options) {
		match self {
			Self::Candidate(n) => (n.id, n.peers, n.log, n.state, n.opts),
			Self::Follower(n) => (n.id, n.peers, n.log, n.state, n.opts),
			Self::Leader(n) => (n.id, n.peers, n.log, n.state, n.opts),
		}
	}

	pub fn force_campaign(self) -> Self {
		match self {
			Self::Follower(n) => n.into_candidate().into(),
			Self::Candidate(mut n) => {
				n.campaign();
				n.into()
			}
			Self::Leader(_) => panic!("leader cannot campaign"),
		}
	}

	pub fn force_heartbeat(&mut self) {
		match self {
			Self::Leader(n) => n.heartbeat(),
			_ => panic!("can only heartbeat as leader"),
		}
	}
}

impl From<RawNode<Candidate>> for Node {
	fn from(n: RawNode<Candidate>) -> Self {
		Node::Candidate(n)
	}
}
impl From<RawNode<Follower>> for Node {
	fn from(n: RawNode<Follower>) -> Self {
		Node::Follower(n)
	}
}
impl From<RawNode<Leader>> for Node {
	fn from(n: RawNode<Leader>) -> Self {
		Node::Leader(n)
	}
}

pub trait Role {}

pub struct RawNode<R: Role> {
	pub(crate) id: NodeId,
	pub(crate) peers: HashSet<NodeId>,
	pub(crate) log: Log,
	pub(crate) state: Box<dyn State>,
	pub(crate) opts: Options,
	pub(crate) role: R,

	pub(crate) outbox: Vec<Envelope>,

	pub(crate) rng: SmallRng,
}

impl<R: Role> RawNode<R> {
	fn into_role<T: Role>(self, role: T) -> RawNode<T> {
		RawNode {
			id: self.id,
			peers: self.peers,
			log: self.log,
			state: self.state,
			opts: self.opts,
			role,
			outbox: self.outbox,
			rng: self.rng,
		}
	}

	fn term(&self) -> Term {
		self.log.get_term_vote().0
	}

	fn cluster_size(&self) -> usize {
		self.peers.len() + 1
	}

	fn quorum_size(&self) -> usize {
		self.cluster_size() / 2 + 1
	}

	fn quorum_value<T: Ord + Copy>(&self, mut values: Vec<T>) -> T {
		assert_eq!(values.len(), self.cluster_size(), "vector size must match cluster size");
		*values.select_nth_unstable_by(self.quorum_size() - 1, |a, b| a.cmp(b).reverse()).1
	}

	fn random_election_timeout(&mut self) -> Ticks {
		self.rng.random_range(self.opts.election_timeout_range.clone())
	}

	fn send(&mut self, to: NodeId, message: Message) {
		self.outbox.push(Envelope {
			from: self.id,
			to,
			term: self.term(),
			message,
		});
	}

	fn broadcast(&mut self, message: Message) {
		let mut peers: Vec<NodeId> = self.peers.iter().copied().collect();
		peers.sort();
		for id in peers {
			self.send(id, message.clone());
		}
	}
}

pub struct Follower {
	pub(crate) leader: Option<NodeId>,
	leader_seen: Ticks,
	election_timeout: Ticks,
}

impl Follower {
	fn new(leader: Option<NodeId>, election_timeout: Ticks) -> Self {
		Self {
			leader,
			leader_seen: 0,
			election_timeout,
		}
	}
}

impl Role for Follower {}

impl RawNode<Follower> {
	fn new(
		id: NodeId,
		peers: HashSet<NodeId>,
		log: Log,
		state: Box<dyn State>,
		opts: Options,
		rng: SmallRng,
	) -> Self {
		assert!(!peers.contains(&id), "node ID {id} can't be in peers");
		let role = Follower::new(None, 0);
		let mut node = Self {
			id,
			peers,
			log,
			state,
			opts,
			role,
			outbox: Vec::new(),
			rng,
		};
		node.role.election_timeout = node.random_election_timeout();
		node.maybe_apply();
		node
	}

	fn into_candidate(mut self) -> RawNode<Candidate> {
		self.maybe_apply();
		let election_timeout = self.random_election_timeout();
		let mut node = self.into_role(Candidate::new(election_timeout));
		node.campaign();
		node
	}

	fn into_follower(mut self, term: Term, leader: Option<NodeId>) -> RawNode<Follower> {
		assert_ne!(term, 0, "can't become follower in term 0");

		if let Some(leader) = leader {
			assert!(self.peers.contains(&leader), "leader is not a peer");
			assert_eq!(self.role.leader, None, "already have leader in term");
			assert_eq!(term, self.term(), "can't follow leader in different term");
			self.role = Follower::new(Some(leader), self.role.election_timeout);
		} else {
			assert_ne!(term, self.term(), "can't become leaderless follower in current term");
			self.log.set_term_vote(term, None);
			self.role = Follower::new(None, self.random_election_timeout());
		}
		self
	}

	fn step(mut self, msg: Envelope) -> Node {
		if msg.term < self.term() {
			return self.into();
		}
		if msg.term > self.term() {
			return self.into_follower(msg.term, None).step(msg);
		}

		if Some(msg.from) == self.role.leader {
			self.role.leader_seen = 0;
		}

		reifydb_assertions! {
			let node_term = self.term();
			assert_eq!(
				msg.term, node_term,
				"follower::step reached message dispatch with msg.term != node term; the term guard above \
				 must equalise terms (lower returns, higher converts) before any arm runs, otherwise \
				 into_follower / set_term_vote would record an out-of-term leader or vote and corrupt the \
				 election state machine (msg.term={}, node_term={node_term})",
				msg.term
			);
		}

		match msg.message {
			Message::Heartbeat {
				last_index,
				commit_index,
			} => self.handle_heartbeat(msg.from, msg.term, last_index, commit_index),

			Message::Append {
				base_index,
				base_term,
				entries,
			} => self.handle_append(msg.from, msg.term, base_index, base_term, entries),

			Message::Campaign {
				last_index,
				last_term,
			} => self.handle_campaign_vote(msg.from, msg.term, last_index, last_term),

			Message::CampaignResponse {
				..
			} => self.into(),

			Message::HeartbeatResponse {
				..
			}
			| Message::AppendResponse {
				..
			} => {
				panic!("follower received unexpected message {msg:?}")
			}
		}
	}

	#[inline]
	fn handle_heartbeat(mut self, from: NodeId, term: Term, last_index: Index, commit_index: Index) -> Node {
		assert!(commit_index <= last_index, "commit_index after last_index");

		match self.role.leader {
			Some(leader) => assert_eq!(from, leader, "multiple leaders in term"),
			None => self = self.into_follower(term, Some(from)),
		}

		let match_index = if self.log.has(last_index, term) {
			last_index
		} else {
			0
		};
		self.send(
			from,
			Message::HeartbeatResponse {
				match_index,
			},
		);

		if match_index != 0 && commit_index > self.log.get_commit_index().0 {
			self.log.commit(commit_index);
			self.maybe_apply();
		}
		self.into()
	}

	#[inline]
	fn handle_append(
		mut self,
		from: NodeId,
		term: Term,
		base_index: Index,
		base_term: Term,
		entries: Vec<Entry>,
	) -> Node {
		if let Some(first) = entries.first() {
			assert_eq!(base_index, first.index - 1, "base index mismatch");
		}

		match self.role.leader {
			Some(leader) => assert_eq!(from, leader, "multiple leaders in term"),
			None => self = self.into_follower(term, Some(from)),
		}

		if base_index == 0 || self.log.has(base_index, base_term) {
			let match_index = entries.last().map(|e| e.index).unwrap_or(base_index);
			self.log.splice(entries);
			self.send(
				from,
				Message::AppendResponse {
					match_index,
					reject_index: 0,
				},
			);
		} else {
			let reject_index = min(base_index, self.log.get_last_index().0 + 1);
			self.send(
				from,
				Message::AppendResponse {
					reject_index,
					match_index: 0,
				},
			);
		}
		self.into()
	}

	#[inline]
	fn handle_campaign_vote(mut self, from: NodeId, term: Term, last_index: Index, last_term: Term) -> Node {
		if let (_, Some(vote)) = self.log.get_term_vote()
			&& from != vote
		{
			self.send(
				from,
				Message::CampaignResponse {
					vote: false,
				},
			);
			return self.into();
		}

		let (log_index, log_term) = self.log.get_last_index();
		if log_term > last_term || (log_term == last_term && log_index > last_index) {
			self.send(
				from,
				Message::CampaignResponse {
					vote: false,
				},
			);
			return self.into();
		}

		self.log.set_term_vote(term, Some(from));
		self.send(
			from,
			Message::CampaignResponse {
				vote: true,
			},
		);
		self.into()
	}

	fn tick(mut self) -> Node {
		self.role.leader_seen += 1;
		if self.role.leader_seen >= self.role.election_timeout {
			return self.into_candidate().into();
		}
		self.into()
	}

	fn maybe_apply(&mut self) {
		let entries = self.log.scan_apply(self.state.get_applied_index());
		for entry in &entries {
			self.state.apply(entry);
		}
	}
}

pub struct Candidate {
	votes: HashSet<NodeId>,
	election_duration: Ticks,
	election_timeout: Ticks,
}

impl Candidate {
	fn new(election_timeout: Ticks) -> Self {
		Self {
			votes: HashSet::new(),
			election_duration: 0,
			election_timeout,
		}
	}
}

impl Role for Candidate {}

impl RawNode<Candidate> {
	fn into_follower(mut self, term: Term, leader: Option<NodeId>) -> RawNode<Follower> {
		let election_timeout = self.random_election_timeout();
		if let Some(leader) = leader {
			assert_eq!(term, self.term(), "can't follow leader in different term");
			self.into_role(Follower::new(Some(leader), election_timeout))
		} else {
			assert_ne!(term, self.term(), "can't become leaderless follower in current term");
			self.log.set_term_vote(term, None);
			self.into_role(Follower::new(None, election_timeout))
		}
	}

	fn into_leader(self) -> RawNode<Leader> {
		let (term, vote) = self.log.get_term_vote();
		assert_ne!(term, 0, "leaders can't have term 0");
		assert_eq!(vote, Some(self.id), "leader did not vote for self");

		let peers = self.peers.clone();
		let (last_index, _) = self.log.get_last_index();
		let mut node = self.into_role(Leader::new(peers, last_index));

		node.propose(Command::Noop);
		node.maybe_commit_and_apply();
		node.heartbeat();

		node
	}

	fn step(mut self, msg: Envelope) -> Node {
		if msg.term < self.term() {
			return self.into();
		}
		if msg.term > self.term() {
			return self.into_follower(msg.term, None).step(msg);
		}

		match msg.message {
			Message::CampaignResponse {
				vote: true,
			} => {
				self.role.votes.insert(msg.from);
				if self.role.votes.len() >= self.quorum_size() {
					return self.into_leader().into();
				}
			}

			Message::CampaignResponse {
				vote: false,
			} => {}

			Message::Campaign {
				..
			} => {
				self.send(
					msg.from,
					Message::CampaignResponse {
						vote: false,
					},
				);
			}

			Message::Heartbeat {
				..
			}
			| Message::Append {
				..
			} => {
				return self.into_follower(msg.term, Some(msg.from)).step(msg);
			}

			Message::HeartbeatResponse {
				..
			}
			| Message::AppendResponse {
				..
			} => {
				panic!("candidate received unexpected message {msg:?}")
			}
		}
		self.into()
	}

	fn tick(mut self) -> Node {
		self.role.election_duration += 1;
		if self.role.election_duration >= self.role.election_timeout {
			self.campaign();
		}
		self.into()
	}

	fn campaign(&mut self) {
		let term = self.term() + 1;
		self.role = Candidate::new(self.random_election_timeout());
		self.role.votes.insert(self.id);
		self.log.set_term_vote(term, Some(self.id));

		let (last_index, last_term) = self.log.get_last_index();
		self.broadcast(Message::Campaign {
			last_index,
			last_term,
		});
	}
}

pub struct Leader {
	pub(crate) progress: HashMap<NodeId, Progress>,
	since_heartbeat: Ticks,
}

pub struct Progress {
	pub match_index: Index,

	pub next_index: Index,
}

impl Progress {
	fn advance(&mut self, match_index: Index) -> bool {
		if match_index <= self.match_index {
			return false;
		}
		self.match_index = match_index;
		self.next_index = max(self.next_index, match_index + 1);
		true
	}

	fn regress_next(&mut self, next_index: Index) -> bool {
		if next_index >= self.next_index || self.next_index <= self.match_index + 1 {
			return false;
		}
		self.next_index = max(next_index, self.match_index + 1);
		true
	}
}

impl Leader {
	fn new(peers: HashSet<NodeId>, last_index: Index) -> Self {
		let next_index = last_index + 1;
		let progress = peers
			.into_iter()
			.map(|p| {
				(
					p,
					Progress {
						next_index,
						match_index: 0,
					},
				)
			})
			.collect();
		Self {
			progress,
			since_heartbeat: 0,
		}
	}
}

impl Role for Leader {}

impl RawNode<Leader> {
	fn into_follower(mut self, term: Term) -> RawNode<Follower> {
		assert!(term > self.term(), "leader can only become follower in later term");
		self.log.set_term_vote(term, None);
		let election_timeout = self.random_election_timeout();
		self.into_role(Follower::new(None, election_timeout))
	}

	fn step(mut self, msg: Envelope) -> Node {
		if msg.term < self.term() {
			return self.into();
		}
		if msg.term > self.term() {
			return self.into_follower(msg.term).step(msg);
		}

		reifydb_assertions! {
			let node_term = self.term();
			assert_eq!(
				msg.term, node_term,
				"leader::step reached message dispatch with msg.term != node term; the term guard above \
				 must equalise terms (lower returns, higher steps down) before any arm runs, otherwise a \
				 leader would process progress/append responses from a foreign term and advance match/next \
				 indices against the wrong log epoch (msg.term={}, node_term={node_term})",
				msg.term
			);
		}

		match msg.message {
			Message::HeartbeatResponse {
				match_index,
			} => self.handle_heartbeat_response(msg.from, match_index),

			Message::AppendResponse {
				match_index,
				reject_index: 0,
			} if match_index > 0 => self.handle_append_accept(msg.from, match_index),

			Message::AppendResponse {
				reject_index,
				match_index: 0,
			} if reject_index > 0 => self.handle_append_reject(msg.from, reject_index),

			Message::AppendResponse {
				..
			} => panic!("invalid message {msg:?}"),

			Message::Campaign {
				..
			} => {
				self.send(
					msg.from,
					Message::CampaignResponse {
						vote: false,
					},
				);
			}

			Message::CampaignResponse {
				..
			} => {}

			Message::Heartbeat {
				..
			}
			| Message::Append {
				..
			} => {
				panic!("saw other leader {} in term {}", msg.from, msg.term);
			}
		}

		self.into()
	}

	#[inline]
	fn handle_heartbeat_response(&mut self, from: NodeId, match_index: Index) {
		let (last_index, _) = self.log.get_last_index();
		assert!(match_index <= last_index, "future match index");

		if match_index == 0 {
			self.progress(from).regress_next(last_index);
			self.maybe_send_append(from, true);
		}

		if self.progress(from).advance(match_index) {
			self.maybe_commit_and_apply();
		}
	}

	#[inline]
	fn handle_append_accept(&mut self, from: NodeId, match_index: Index) {
		let (last_index, _) = self.log.get_last_index();
		assert!(match_index <= last_index, "future match index");

		if self.progress(from).advance(match_index) {
			self.maybe_commit_and_apply();
		}

		self.maybe_send_append(from, false);
	}

	#[inline]
	fn handle_append_reject(&mut self, from: NodeId, reject_index: Index) {
		let (last_index, _) = self.log.get_last_index();
		assert!(reject_index <= last_index, "future reject index");

		if reject_index <= self.progress(from).match_index {
			return;
		}

		if self.progress(from).regress_next(reject_index) {
			self.maybe_send_append(from, true);
		}
	}

	fn tick(mut self) -> Node {
		self.role.since_heartbeat += 1;
		if self.role.since_heartbeat >= self.opts.heartbeat_interval {
			self.heartbeat();
		}
		self.into()
	}

	fn heartbeat(&mut self) {
		let (last_index, last_term) = self.log.get_last_index();
		let (commit_index, _) = self.log.get_commit_index();
		assert_eq!(last_term, self.term(), "leader's last_term not in current term");

		self.role.since_heartbeat = 0;
		self.broadcast(Message::Heartbeat {
			last_index,
			commit_index,
		});
	}

	pub(crate) fn propose(&mut self, command: Command) -> Index {
		let index = self.log.append(command);
		let mut peers: Vec<NodeId> = self.peers.iter().copied().collect();
		peers.sort();
		for peer in peers {
			if index == self.progress(peer).next_index {
				self.maybe_send_append(peer, false);
			}
		}
		index
	}

	fn maybe_commit_and_apply(&mut self) -> Index {
		let (last_index, _) = self.log.get_last_index();
		let commit_index = self
			.quorum_value(self.role.progress.values().map(|p| p.match_index).chain([last_index]).collect());

		let (old_index, _) = self.log.get_commit_index();
		if commit_index <= old_index {
			return old_index;
		}

		match self.log.get(commit_index) {
			Some(entry) if entry.term == self.term() => {}
			Some(_) => return old_index,
			None => panic!("commit index {commit_index} missing"),
		}

		self.log.commit(commit_index);

		let entries = self.log.scan_apply(self.state.get_applied_index());
		for entry in &entries {
			self.state.apply(entry);
		}

		commit_index
	}

	fn maybe_send_append(&mut self, peer: NodeId, mut probe: bool) {
		let (last_index, _) = self.log.get_last_index();
		let progress = self.role.progress.get_mut(&peer).expect("unknown node");
		assert_ne!(progress.next_index, 0, "invalid next_index");

		if progress.match_index == last_index {
			return;
		}

		probe = probe && progress.next_index > progress.match_index + 1;

		if progress.next_index > last_index && !probe {
			return;
		}

		let (base_index, base_term) = match progress.next_index {
			0 => panic!("next_index=0 for node {peer}"),
			1 => (0, 0),
			next => self.log.get(next - 1).map(|e| (e.index, e.term)).expect("missing base entry"),
		};

		let entries = if probe {
			Vec::new()
		} else {
			self.log.scan(progress.next_index, last_index)
				.into_iter()
				.take(self.opts.max_append_entries)
				.collect()
		};

		if let Some(last) = entries.last() {
			progress.next_index = last.index + 1;
		}

		self.send(
			peer,
			Message::Append {
				base_index,
				base_term,
				entries,
			},
		);
	}

	fn progress(&mut self, id: NodeId) -> &mut Progress {
		self.role.progress.get_mut(&id).expect("unknown node")
	}
}
