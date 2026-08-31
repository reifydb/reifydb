// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::{Path, PathBuf};

use reifydb_codec::log::{
	VoteSeq,
	vote::{FILE_BYTES, SLOT_BYTES, SLOTS, State},
};
use reifydb_runtime::io::fs::{Create, Filesystem, Len, Open, OpenMut, Pread, SyncData, SyncDir};

use crate::{
	error::{LogError, Result},
	segment::{read_exact, write_all},
};

pub struct Vote<F: Filesystem> {
	path: PathBuf,
	file: F::FileMut,
	state: State,
	seq: VoteSeq,
	next: usize,
}

impl<F: Filesystem> Vote<F> {
	pub fn create(fs: &F, path: &Path) -> Result<Self>
	where
		F: Create + SyncDir,
	{
		let file = fs.create(path, FILE_BYTES as u64)?;
		write_all(&file, path, 0, &State::EMPTY.encode(VoteSeq::FIRST))?;
		file.sync_data()?;
		fs.sync_dir(parent(path))?;
		Ok(Self {
			path: path.to_path_buf(),
			file,
			state: State::EMPTY,
			seq: VoteSeq::FIRST,
			next: 1,
		})
	}

	pub fn open(fs: &F, path: &Path) -> Result<Self>
	where
		F: OpenMut,
	{
		let file = fs.open_mut(path)?;
		let len = file.len()?;
		let (state, seq, winner) = load(&file, path, len)?;
		Ok(Self {
			path: path.to_path_buf(),
			file,
			state,
			seq,
			next: (winner + 1) % SLOTS,
		})
	}

	pub fn save(&mut self, state: State) -> Result<()> {
		let seq = self.seq.next();
		write_all(&self.file, &self.path, (self.next * SLOT_BYTES) as u64, &state.encode(seq))?;
		self.file.sync_data()?;
		self.state = state;
		self.seq = seq;
		self.next = (self.next + 1) % SLOTS;
		Ok(())
	}

	pub fn state(&self) -> State {
		self.state
	}

	pub fn seq(&self) -> VoteSeq {
		self.seq
	}

	pub fn path(&self) -> &Path {
		&self.path
	}
}

pub fn read<F: Open>(fs: &F, path: &Path) -> Result<State> {
	let file = fs.open(path)?;
	let len = file.len()?;
	Ok(load(&file, path, len)?.0)
}

fn load<H: Pread>(file: &H, path: &Path, len: u64) -> Result<(State, VoteSeq, usize)> {
	if len < FILE_BYTES as u64 {
		return Err(LogError::VoteShort {
			path: path.to_path_buf(),
			len,
		});
	}
	let mut best: Option<(State, VoteSeq, usize)> = None;
	for slot in 0..SLOTS {
		let mut raw = [0u8; SLOT_BYTES];
		if !read_exact(file, (slot * SLOT_BYTES) as u64, &mut raw)? {
			continue;
		}
		let Some((state, seq)) = State::decode(&raw) else {
			continue;
		};
		if best.is_none_or(|(_, found, _)| seq > found) {
			best = Some((state, seq, slot));
		}
	}
	best.ok_or_else(|| LogError::VoteCorrupt {
		path: path.to_path_buf(),
	})
}

fn parent(path: &Path) -> &Path {
	match path.parent() {
		Some(parent) if !parent.as_os_str().is_empty() => parent,
		_ => Path::new("."),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::log::{LogIndex, NodeId, Term};
	use reifydb_runtime::io::fs::{Mkdir, Pwrite, memory::MemoryFs};

	use super::*;

	fn fixture() -> (MemoryFs, PathBuf) {
		let fs = MemoryFs::new();
		fs.mkdir(Path::new("/log")).unwrap();
		(fs, PathBuf::from("/log/vote"))
	}

	fn state(term: u64, voted_for: u64, commit_index: u64) -> State {
		State {
			term: Term::new(term),
			voted_for: Some(NodeId::new(voted_for)),
			commit_index: LogIndex::new(commit_index),
		}
	}

	fn poke(fs: &MemoryFs, path: &Path, offset: u64, bytes: &[u8]) {
		let file = fs.open_mut(path).unwrap();
		assert_eq!(file.pwrite(offset, bytes).unwrap(), bytes.len());
	}

	#[test]
	fn a_fresh_vote_file_reads_back_as_no_vote_at_term_zero() {
		// a node that has never voted must say so explicitly, not decode as having voted for node 0.
		let (fs, path) = fixture();
		let vote = Vote::create(&fs, &path).unwrap();

		assert_eq!(vote.state(), State::EMPTY);
		assert_eq!(read(&fs, &path).unwrap(), State::EMPTY);
		assert_eq!(State::EMPTY.voted_for, None);
	}

	#[test]
	fn a_saved_vote_survives_a_reopen() {
		let (fs, path) = fixture();
		let mut vote = Vote::<MemoryFs>::create(&fs, &path).unwrap();
		let saved = state(4, 2, 9);
		vote.save(saved).unwrap();

		assert_eq!(Vote::<MemoryFs>::open(&fs, &path).unwrap().state(), saved);
	}

	#[test]
	fn saves_alternate_slots_so_the_live_one_is_never_the_one_being_written() {
		// overwriting the slot that currently holds the vote is what makes a torn write fatal.
		let (fs, path) = fixture();
		let mut vote = Vote::<MemoryFs>::create(&fs, &path).unwrap();
		vote.save(state(1, 1, 0)).unwrap();
		vote.save(state(2, 2, 0)).unwrap();

		let mut first = [0u8; SLOT_BYTES];
		let mut second = [0u8; SLOT_BYTES];
		let file = fs.open(&path).unwrap();
		read_exact(&file, 0, &mut first).unwrap();
		read_exact(&file, SLOT_BYTES as u64, &mut second).unwrap();

		assert_eq!(State::decode(&first).unwrap().0.term, Term::new(2));
		assert_eq!(State::decode(&second).unwrap().0.term, Term::new(1));
	}

	#[test]
	fn a_torn_write_of_the_newest_slot_falls_back_to_the_previous_vote() {
		// this is the whole reason there are two slots: a half written vote must cost the
		// last update, never the vote itself.
		let (fs, path) = fixture();
		let mut vote = Vote::<MemoryFs>::create(&fs, &path).unwrap();
		let older = state(1, 1, 0);
		vote.save(older).unwrap();
		vote.save(state(2, 2, 0)).unwrap();

		poke(&fs, &path, 12, &[0xff; 4]);

		assert_eq!(read(&fs, &path).unwrap(), older);
	}

	#[test]
	fn a_vote_whose_slots_both_fail_is_an_error_and_never_term_zero() {
		// silently resetting to term zero lets a node vote twice in a term it already voted in.
		let (fs, path) = fixture();
		let mut vote = Vote::<MemoryFs>::create(&fs, &path).unwrap();
		vote.save(state(5, 1, 0)).unwrap();

		poke(&fs, &path, 12, &[0xff; 4]);
		poke(&fs, &path, SLOT_BYTES as u64 + 12, &[0xff; 4]);

		assert!(matches!(read(&fs, &path), Err(LogError::VoteCorrupt { .. })));
	}

	#[test]
	fn a_file_shorter_than_both_slots_is_an_error() {
		let fs = MemoryFs::new();
		fs.mkdir(Path::new("/log")).unwrap();
		let path = PathBuf::from("/log/vote");
		fs.create(&path, SLOT_BYTES as u64).unwrap();

		assert!(matches!(read(&fs, &path), Err(LogError::VoteShort { .. })));
	}

	#[test]
	fn the_newest_sequence_wins_regardless_of_which_slot_holds_it() {
		// slot order is an implementation detail; the sequence number is the durable fact
		// about which write landed last, and inferring it from slot order resurrects old votes.
		let (fs, path) = fixture();
		let newer = state(9, 3, 1);
		let older = state(2, 4, 1);

		fs.create(&path, FILE_BYTES as u64).unwrap();
		poke(&fs, &path, 0, &newer.encode(VoteSeq::new(8)));
		poke(&fs, &path, SLOT_BYTES as u64, &older.encode(VoteSeq::new(7)));
		assert_eq!(read(&fs, &path).unwrap(), newer);

		poke(&fs, &path, 0, &older.encode(VoteSeq::new(7)));
		poke(&fs, &path, SLOT_BYTES as u64, &newer.encode(VoteSeq::new(8)));
		assert_eq!(read(&fs, &path).unwrap(), newer);
	}

	#[test]
	fn a_rewritten_vote_at_the_same_term_wins_on_its_sequence_alone() {
		// term and commit index can both stand still while voted_for moves, and with nothing
		// but those two to compare the older slot wins and the node votes twice in one term.
		let (fs, path) = fixture();
		let first = state(4, 1, 2);
		let second = state(4, 2, 2);

		fs.create(&path, FILE_BYTES as u64).unwrap();
		poke(&fs, &path, 0, &second.encode(VoteSeq::new(11)));
		poke(&fs, &path, SLOT_BYTES as u64, &first.encode(VoteSeq::new(10)));

		assert_eq!(read(&fs, &path).unwrap(), second);
	}

	#[test]
	fn the_sequence_advances_by_one_on_every_save() {
		// a sequence that stalls makes two slots indistinguishable, which is the whole failure
		// this field exists to remove.
		let (fs, path) = fixture();
		let mut vote = Vote::<MemoryFs>::create(&fs, &path).unwrap();
		let start = vote.seq();
		for term in 1..=4u64 {
			vote.save(state(term, term, 0)).unwrap();
		}

		assert_eq!(vote.seq(), VoteSeq::new(start.as_u64() + 4));
		assert_eq!(Vote::<MemoryFs>::open(&fs, &path).unwrap().seq(), VoteSeq::new(start.as_u64() + 4));
	}
}
