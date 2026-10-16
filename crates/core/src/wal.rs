// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_value::util::cowvec::CowVec;
use reifydb_wal::{
	body::Body,
	error::{Result, WalError},
};

use crate::{
	common::CommitVersion,
	delta::{Delta, RemoveAnnounce, SetPre},
	key::any::TaggedKey,
};

const KIND: u32 = 1;

const OP_SET: u8 = 0;
const OP_REMOVE: u8 = 1;

const PRE_ABSENT: u8 = 0;
const PRE_PRESENT: u8 = 1;

const ANNOUNCE_SILENT: u8 = 0;
const ANNOUNCE_ANNOUNCED: u8 = 1;
const ANNOUNCE_UNOBSERVED: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
	pub version: CommitVersion,
	pub deltas: Vec<Delta>,
}

impl Body for CommitRecord {
	fn kind(&self) -> u32 {
		KIND
	}

	fn encode(&self, out: &mut Vec<u8>) {
		out.extend_from_slice(&self.version.0.to_le_bytes());
		put_len(out, self.deltas.len());
		for delta in &self.deltas {
			match delta {
				Delta::Set {
					key,
					bytes,
					pre,
				} => {
					out.push(OP_SET);
					put_bytes(out, key.encode().as_bytes());
					put_bytes(out, bytes.as_ref());
					match pre {
						SetPre::Absent => out.push(PRE_ABSENT),
						SetPre::Present(pre) => {
							out.push(PRE_PRESENT);
							put_bytes(out, pre.as_ref());
						}
						SetPre::Unknown => panic!(
							"commit {} has an unresolved pre for key {:?}",
							self.version.0, key
						),
					}
				}
				Delta::Remove {
					key,
					announce,
				} => {
					out.push(OP_REMOVE);
					put_bytes(out, key.encode().as_bytes());
					match announce {
						RemoveAnnounce::Silent => out.push(ANNOUNCE_SILENT),
						RemoveAnnounce::Announced {
							pre,
						} => {
							out.push(ANNOUNCE_ANNOUNCED);
							put_bytes(out, pre.as_ref());
						}
						RemoveAnnounce::Unobserved {
							pre,
						} => {
							out.push(ANNOUNCE_UNOBSERVED);
							put_bytes(out, pre.as_ref());
						}
					}
				}
			}
		}
	}

	fn decode(kind: u32, bytes: &[u8]) -> Result<Self> {
		if kind != KIND {
			return Err(WalError::Body(format!("unknown commit record kind {kind}")));
		}
		let mut reader = Reader {
			bytes,
			at: 0,
		};
		let version = CommitVersion(reader.u64()?);
		let count = reader.u32()?;
		let mut deltas = Vec::new();
		for _ in 0..count {
			deltas.push(reader.delta()?);
		}
		if reader.at != bytes.len() {
			return Err(WalError::Body(format!(
				"{} trailing bytes after delta {count}",
				bytes.len() - reader.at
			)));
		}
		Ok(Self {
			version,
			deltas,
		})
	}
}

fn put_len(out: &mut Vec<u8>, len: usize) {
	let len = u32::try_from(len).unwrap_or_else(|_| panic!("commit record length {len} does not fit in u32"));
	out.extend_from_slice(&len.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
	put_len(out, bytes.len());
	out.extend_from_slice(bytes);
}

struct Reader<'a> {
	bytes: &'a [u8],
	at: usize,
}

impl<'a> Reader<'a> {
	fn take(&mut self, len: usize) -> Result<&'a [u8]> {
		let end = self.at.checked_add(len).filter(|end| *end <= self.bytes.len()).ok_or_else(|| {
			WalError::Body(format!(
				"truncated at byte {}: needs {len} of {} remaining",
				self.at,
				self.bytes.len() - self.at
			))
		})?;
		let slice = &self.bytes[self.at..end];
		self.at = end;
		Ok(slice)
	}

	fn u8(&mut self) -> Result<u8> {
		Ok(self.take(1)?[0])
	}

	fn u32(&mut self) -> Result<u32> {
		let mut buf = [0u8; 4];
		buf.copy_from_slice(self.take(4)?);
		Ok(u32::from_le_bytes(buf))
	}

	fn u64(&mut self) -> Result<u64> {
		let mut buf = [0u8; 8];
		buf.copy_from_slice(self.take(8)?);
		Ok(u64::from_le_bytes(buf))
	}

	fn bytes(&mut self) -> Result<&'a [u8]> {
		let len = self.u32()? as usize;
		self.take(len)
	}

	fn encoded(&mut self) -> Result<EncodedBytes> {
		Ok(EncodedBytes(CowVec::new(self.bytes()?.to_vec())))
	}

	fn key(&mut self) -> Result<TaggedKey> {
		let at = self.at;
		let key = EncodedKey::new(self.bytes()?);
		TaggedKey::decode(&key).ok_or_else(|| WalError::Body(format!("undecodable key at byte {at}")))
	}

	fn delta(&mut self) -> Result<Delta> {
		let at = self.at;
		match self.u8()? {
			OP_SET => {
				let key = self.key()?;
				let bytes = self.encoded()?;
				let pre_at = self.at;
				let pre = match self.u8()? {
					PRE_ABSENT => SetPre::Absent,
					PRE_PRESENT => SetPre::Present(self.encoded()?),
					tag => {
						return Err(WalError::Body(format!(
							"unknown pre tag {tag} at byte {pre_at}"
						)));
					}
				};
				Ok(Delta::Set {
					key,
					bytes,
					pre,
				})
			}
			OP_REMOVE => {
				let key = self.key()?;
				let announce_at = self.at;
				let announce = match self.u8()? {
					ANNOUNCE_SILENT => RemoveAnnounce::Silent,
					ANNOUNCE_ANNOUNCED => RemoveAnnounce::Announced {
						pre: self.encoded()?,
					},
					ANNOUNCE_UNOBSERVED => RemoveAnnounce::Unobserved {
						pre: self.encoded()?,
					},
					tag => {
						return Err(WalError::Body(format!(
							"unknown announce tag {tag} at byte {announce_at}"
						)));
					}
				};
				Ok(Delta::Remove {
					key,
					announce,
				})
			}
			op => Err(WalError::Body(format!("unknown delta op {op} at byte {at}"))),
		}
	}
}
