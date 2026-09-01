// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::{Path, PathBuf};

use reifydb_codec::log::{
	LogIndex, VoteSeq,
	vote::{FILE_BYTES, SLOT_BYTES, SLOTS, State},
};
use reifydb_runtime::io::fs::{Create, Filesystem, Len, Open, OpenMut, Pread, SyncData, SyncDir};

use crate::{
	error::{LogError, Result},
	segment::{read_exact, write_all},
};

pub const VOTE_NAMES: [&str; SLOTS] = ["vote.a", "vote.b"];

pub struct Vote<F: Filesystem> {
	dir: PathBuf,
	files: [F::FileMut; SLOTS],
	state: State,
	seq: VoteSeq,
	next: usize,
}

impl<F: Filesystem> Vote<F> {
	pub fn create(fs: &F, dir: &Path) -> Result<Self>
	where
		F: Create + SyncDir,
	{
		let seq = VoteSeq::FIRST.next();
		let second = fresh(fs, dir, 1, VoteSeq::FIRST)?;
		let first = fresh(fs, dir, 0, seq)?;
		fs.sync_dir(dir)?;
		Ok(Self {
			dir: dir.to_path_buf(),
			files: [first, second],
			state: State::EMPTY,
			seq,
			next: 1,
		})
	}

	pub fn open(fs: &F, dir: &Path) -> Result<Self>
	where
		F: OpenMut,
	{
		let files = [fs.open_mut(&dir.join(VOTE_NAMES[0]))?, fs.open_mut(&dir.join(VOTE_NAMES[1]))?];
		let (state, seq, winner) = pick(&files, dir)?;
		Ok(Self {
			dir: dir.to_path_buf(),
			files,
			state,
			seq,
			next: (winner + 1) % SLOTS,
		})
	}

	pub fn save(&mut self, state: State) -> Result<()> {
		let seq = self.seq.next();
		let path = self.dir.join(VOTE_NAMES[self.next]);
		write_all(&self.files[self.next], &path, 0, &state.encode(seq))?;
		self.files[self.next].sync_data()?;
		self.state = state;
		self.seq = seq;
		self.next = (self.next + 1) % SLOTS;
		Ok(())
	}

	pub fn advance(&mut self, commit_index: LogIndex) {
		self.state.commit_index = commit_index;
	}

	pub fn state(&self) -> State {
		self.state
	}

	pub fn seq(&self) -> VoteSeq {
		self.seq
	}

	pub fn dir(&self) -> &Path {
		&self.dir
	}
}

pub fn read<F: Open>(fs: &F, dir: &Path) -> Result<State> {
	let files = [fs.open(&dir.join(VOTE_NAMES[0]))?, fs.open(&dir.join(VOTE_NAMES[1]))?];
	Ok(pick(&files, dir)?.0)
}

fn fresh<F: Filesystem + Create>(fs: &F, dir: &Path, at: usize, seq: VoteSeq) -> Result<F::FileMut> {
	let path = dir.join(VOTE_NAMES[at]);
	let file = fs.create(&path, FILE_BYTES as u64)?;
	write_all(&file, &path, 0, &State::EMPTY.encode(seq))?;
	file.sync_data()?;
	Ok(file)
}

enum Slot {
	Valid(State, VoteSeq),
	Short(u64),
	Corrupt,
}

fn pick<H: Pread + Len>(files: &[H; SLOTS], dir: &Path) -> Result<(State, VoteSeq, usize)> {
	let mut best: Option<(State, VoteSeq, usize)> = None;
	let mut short: Option<u64> = None;
	for (at, file) in files.iter().enumerate() {
		match slot(file)? {
			Slot::Valid(state, seq) => {
				if best.is_none_or(|(_, found, _)| seq > found) {
					best = Some((state, seq, at));
				}
			}
			Slot::Short(len) => short = Some(short.map_or(len, |found: u64| found.min(len))),
			Slot::Corrupt => {}
		}
	}
	match (best, short) {
		(Some(found), _) => Ok(found),
		(None, Some(len)) => Err(LogError::VoteShort {
			path: dir.to_path_buf(),
			len,
		}),
		(None, None) => Err(LogError::VoteCorrupt {
			path: dir.to_path_buf(),
		}),
	}
}

fn slot<H: Pread + Len>(file: &H) -> Result<Slot> {
	let len = file.len()?;
	if len < FILE_BYTES as u64 {
		return Ok(Slot::Short(len));
	}
	let mut raw = [0u8; SLOT_BYTES];
	if !read_exact(file, 0, &mut raw)? {
		return Ok(Slot::Short(len));
	}
	Ok(match State::decode(&raw) {
		Some((state, seq)) => Slot::Valid(state, seq),
		None => Slot::Corrupt,
	})
}

#[cfg(test)]
mod tests {
	use reifydb_codec::log::{LogIndex, NodeId, Term};
	use reifydb_runtime::io::fs::{Mkdir, Pwrite, memory::MemoryFs};

	use super::*;

	const DIR: &str = "/log";

	fn fixture() -> (MemoryFs, PathBuf) {
		let fs = MemoryFs::new();
		fs.mkdir(Path::new(DIR)).unwrap();
		(fs, PathBuf::from(DIR))
	}

	fn slot_path(dir: &Path, at: usize) -> PathBuf {
		dir.join(VOTE_NAMES[at])
	}

	fn state(term: u64, voted_for: u64, commit_index: u64) -> State {
		State {
			term: Term::new(term),
			voted_for: Some(NodeId::new(voted_for)),
			commit_index: LogIndex::new(commit_index),
			..State::EMPTY
		}
	}

	fn poke(fs: &MemoryFs, path: &Path, offset: u64, bytes: &[u8]) {
		let file = fs.open_mut(path).unwrap();
		assert_eq!(file.pwrite(offset, bytes).unwrap(), bytes.len());
	}

	fn lay(fs: &MemoryFs, dir: &Path, at: usize, state: State, seq: VoteSeq) {
		let path = slot_path(dir, at);
		let file = match fs.open_mut(&path) {
			Ok(found) => found,
			Err(_) => fs.create(&path, FILE_BYTES as u64).unwrap(),
		};
		assert_eq!(file.pwrite(0, &state.encode(seq)).unwrap(), SLOT_BYTES);
	}

	fn term_in(fs: &MemoryFs, dir: &Path, at: usize) -> Term {
		let mut raw = [0u8; SLOT_BYTES];
		read_exact(&fs.open(&slot_path(dir, at)).unwrap(), 0, &mut raw).unwrap();
		State::decode(&raw).unwrap().0.term
	}

	#[test]
	fn a_fresh_vote_reads_back_as_no_vote_at_term_zero() {
		// a node that has never voted must say so explicitly, not decode as having voted for node 0.
		let (fs, dir) = fixture();
		let vote = Vote::create(&fs, &dir).unwrap();

		assert_eq!(vote.state(), State::EMPTY);
		assert_eq!(read(&fs, &dir).unwrap(), State::EMPTY);
		assert_eq!(State::EMPTY.voted_for, None);
	}

	#[test]
	fn both_slot_files_are_written_at_create_so_one_bad_write_is_survivable() {
		// leaving the second file uninitialised means a single lost or misdirected write at create
		// leaves no slot that verifies, and an intact log becomes unopenable.
		let (fs, dir) = fixture();
		Vote::<MemoryFs>::create(&fs, &dir).unwrap();

		for at in 0..SLOTS {
			let mut raw = [0u8; SLOT_BYTES];
			read_exact(&fs.open(&slot_path(&dir, at)).unwrap(), 0, &mut raw).unwrap();
			assert!(State::decode(&raw).is_some(), "slot {at} was left unwritten at create");
		}
	}

	#[test]
	fn a_saved_vote_survives_a_reopen() {
		let (fs, dir) = fixture();
		let mut vote = Vote::<MemoryFs>::create(&fs, &dir).unwrap();
		let saved = state(4, 2, 9);
		vote.save(saved).unwrap();

		assert_eq!(Vote::<MemoryFs>::open(&fs, &dir).unwrap().state(), saved);
	}

	#[test]
	fn saves_alternate_files_so_the_live_slot_is_never_the_one_being_written() {
		// overwriting the file that currently holds the vote is what makes a torn write fatal.
		let (fs, dir) = fixture();
		let mut vote = Vote::<MemoryFs>::create(&fs, &dir).unwrap();
		vote.save(state(1, 1, 0)).unwrap();
		vote.save(state(2, 2, 0)).unwrap();

		assert_eq!(term_in(&fs, &dir, 0), Term::new(2));
		assert_eq!(term_in(&fs, &dir, 1), Term::new(1));
	}

	#[test]
	fn a_torn_write_of_the_newest_slot_falls_back_to_the_previous_vote() {
		// this is the whole reason there are two files: a half written vote must cost the
		// last update, never the vote itself.
		let (fs, dir) = fixture();
		let mut vote = Vote::<MemoryFs>::create(&fs, &dir).unwrap();
		let older = state(1, 1, 0);
		vote.save(older).unwrap();
		vote.save(state(2, 2, 0)).unwrap();

		poke(&fs, &slot_path(&dir, 0), 12, &[0xff; 4]);

		assert_eq!(read(&fs, &dir).unwrap(), older);
	}

	#[test]
	fn a_vote_whose_slots_both_fail_is_an_error_and_never_term_zero() {
		// silently resetting to term zero lets a node vote twice in a term it already voted in.
		let (fs, dir) = fixture();
		let mut vote = Vote::<MemoryFs>::create(&fs, &dir).unwrap();
		vote.save(state(5, 1, 0)).unwrap();

		poke(&fs, &slot_path(&dir, 0), 12, &[0xff; 4]);
		poke(&fs, &slot_path(&dir, 1), 12, &[0xff; 4]);

		assert!(matches!(read(&fs, &dir), Err(LogError::VoteCorrupt { .. })));
	}

	#[test]
	fn one_short_slot_file_is_answered_by_the_other_one() {
		// a truncated file is exactly the fault the second file exists to absorb, so refusing the
		// whole vote because one of the two is short throws away the redundancy.
		let (fs, dir) = fixture();
		let saved = state(7, 3, 4);
		lay(&fs, &dir, 1, saved, VoteSeq::new(9));
		fs.create(&slot_path(&dir, 0), 8).unwrap();

		assert_eq!(read(&fs, &dir).unwrap(), saved);
	}

	#[test]
	fn a_vote_whose_slots_are_both_short_is_reported_as_truncated() {
		// with nothing left to read, reporting truncation rather than term zero is what stops a
		// node from voting a second time in a term it has already voted in.
		let (fs, dir) = fixture();
		fs.create(&slot_path(&dir, 0), 8).unwrap();
		fs.create(&slot_path(&dir, 1), 8).unwrap();

		assert!(matches!(read(&fs, &dir), Err(LogError::VoteShort { .. })));
	}

	#[test]
	fn the_newest_sequence_wins_regardless_of_which_file_holds_it() {
		// file order is an implementation detail; the sequence number is the durable fact
		// about which write landed last, and inferring it from file order resurrects old votes.
		let (fs, dir) = fixture();
		let newer = state(9, 3, 1);
		let older = state(2, 4, 1);

		lay(&fs, &dir, 0, newer, VoteSeq::new(8));
		lay(&fs, &dir, 1, older, VoteSeq::new(7));
		assert_eq!(read(&fs, &dir).unwrap(), newer);

		lay(&fs, &dir, 0, older, VoteSeq::new(7));
		lay(&fs, &dir, 1, newer, VoteSeq::new(8));
		assert_eq!(read(&fs, &dir).unwrap(), newer);
	}

	#[test]
	fn a_rewritten_vote_at_the_same_term_wins_on_its_sequence_alone() {
		// term and commit index can both stand still while voted_for moves, and with nothing
		// but those two to compare the older slot wins and the node votes twice in one term.
		let (fs, dir) = fixture();
		let first = state(4, 1, 2);
		let second = state(4, 2, 2);

		lay(&fs, &dir, 0, second, VoteSeq::new(11));
		lay(&fs, &dir, 1, first, VoteSeq::new(10));

		assert_eq!(read(&fs, &dir).unwrap(), second);
	}

	#[test]
	fn the_sequence_advances_by_one_on_every_save() {
		// a sequence that stalls makes two slots indistinguishable, which is the whole failure
		// this field exists to remove.
		let (fs, dir) = fixture();
		let mut vote = Vote::<MemoryFs>::create(&fs, &dir).unwrap();
		let start = vote.seq();
		for term in 1..=4u64 {
			vote.save(state(term, term, 0)).unwrap();
		}

		assert_eq!(vote.seq(), VoteSeq::new(start.as_u64() + 4));
		assert_eq!(Vote::<MemoryFs>::open(&fs, &dir).unwrap().seq(), VoteSeq::new(start.as_u64() + 4));
	}
}
