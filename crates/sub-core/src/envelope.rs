// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryKind {
	Response = 0x00,
	Change = 0x01,
	BatchChange = 0x02,
}

pub fn encode_rbcf_envelope(kind: BinaryKind, id: &str, rbcf_bytes: &[u8], meta_json: Option<&str>) -> Vec<u8> {
	let id_bytes = id.as_bytes();
	let meta_bytes = meta_json.unwrap_or_default().as_bytes();

	let mut envelope = Vec::with_capacity(1 + 4 + id_bytes.len() + 4 + meta_bytes.len() + rbcf_bytes.len());
	envelope.push(kind as u8);
	envelope.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
	envelope.extend_from_slice(id_bytes);
	envelope.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
	envelope.extend_from_slice(meta_bytes);
	envelope.extend_from_slice(rbcf_bytes);
	envelope
}

pub fn encode_rbcf_batch_envelope(batch_id: &str, entries: &[(String, Vec<u8>)]) -> Vec<u8> {
	let batch_id_bytes = batch_id.as_bytes();
	let mut total_entries_bytes = 0usize;
	for (sub_id, rbcf) in entries {
		total_entries_bytes += 4 + sub_id.len() + 4 + rbcf.len();
	}

	let mut envelope = Vec::with_capacity(1 + 4 + batch_id_bytes.len() + 4 + total_entries_bytes);
	envelope.push(BinaryKind::BatchChange as u8);
	envelope.extend_from_slice(&(batch_id_bytes.len() as u32).to_le_bytes());
	envelope.extend_from_slice(batch_id_bytes);
	envelope.extend_from_slice(&(entries.len() as u32).to_le_bytes());
	for (sub_id, rbcf) in entries {
		let sub_id_bytes = sub_id.as_bytes();
		envelope.extend_from_slice(&(sub_id_bytes.len() as u32).to_le_bytes());
		envelope.extend_from_slice(sub_id_bytes);
		envelope.extend_from_slice(&(rbcf.len() as u32).to_le_bytes());
		envelope.extend_from_slice(rbcf);
	}
	envelope
}
