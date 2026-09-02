// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::{
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_macro::Key;
use reifydb_value::value::{dictionary::DictionaryId, sumtype::SumTypeId};

use super::KeyKind;
use crate::{
	interface::catalog::{
		flow::FlowId,
		id::{
			BindingId, HandlerId, NamespaceId, ProcedureId, QueueId, RingBufferId, SeriesId, SinkId,
			SourceId, TableId, ViewId,
		},
	},
	key::typed::key::Key,
};

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = Namespace)]
pub struct NamespaceKey {
	pub namespace: NamespaceId,
}

impl NamespaceKey {
	pub fn encoded(namespace: impl Into<NamespaceId>) -> EncodedKey {
		Self {
			namespace: namespace.into(),
		}
		.encode()
	}

	pub fn full_scan() -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::namespace_start()), Some(Self::namespace_end()))
	}

	fn namespace_start() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8);
		serializer.to_encoded_key()
	}

	fn namespace_end() -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(1);
		serializer.extend_u8(Self::KIND as u8 - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_key_tests {
	use super::NamespaceKey;
	use crate::{interface::catalog::id::NamespaceId, key::typed::key::Key};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceKey {
			namespace: NamespaceId(0xABCD),
		};
		let encoded = key.encode();
		let expected = vec![0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32];
		assert_eq!(encoded.as_slice(), expected);

		let key = NamespaceKey::decode(&encoded).unwrap();
		assert_eq!(key.namespace, 0xABCD);
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceBinding)]
pub struct NamespaceBindingKey {
	pub namespace: NamespaceId,
	pub binding: BindingId,
}

impl NamespaceBindingKey {
	pub fn encoded(namespace: impl Into<NamespaceId>, binding: impl Into<BindingId>) -> EncodedKey {
		Self {
			namespace: namespace.into(),
			binding: binding.into(),
		}
		.encode()
	}

	pub fn full_scan(namespace_id: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace_id)), Some(Self::link_end(namespace_id)))
	}

	fn link_start(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace_id);
		serializer.to_encoded_key()
	}

	fn link_end(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace_id - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_binding_key_tests {
	use super::NamespaceBindingKey;
	use crate::{
		interface::catalog::id::{BindingId, NamespaceId},
		key::typed::key::Key,
	};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceBindingKey {
			namespace: NamespaceId(0xABCD),
			binding: BindingId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();
		let decoded = NamespaceBindingKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.binding, BindingId(0x123456789ABCDEF0));
		assert_eq!(key, decoded);
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceDictionary)]
pub struct NamespaceDictionaryKey {
	pub namespace: NamespaceId,
	pub dictionary: DictionaryId,
}

impl NamespaceDictionaryKey {
	pub fn new(namespace: NamespaceId, dictionary: DictionaryId) -> Self {
		Self {
			namespace,
			dictionary,
		}
	}

	pub fn encoded(namespace: impl Into<NamespaceId>, dictionary: impl Into<DictionaryId>) -> EncodedKey {
		Self::new(namespace.into(), dictionary.into()).encode()
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace)), Some(Self::link_end(namespace)))
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_dictionary_key_tests {
	use std::ops::Bound;

	use super::*;

	#[test]
	fn test_namespace_dictionary_key_encode_decode() {
		let key = NamespaceDictionaryKey {
			namespace: NamespaceId(1025),
			dictionary: DictionaryId(2048),
		};
		let encoded = key.encode();
		let decoded = NamespaceDictionaryKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, key.namespace);
		assert_eq!(decoded.dictionary, key.dictionary);
	}

	#[test]
	fn test_namespace_dictionary_key_full_scan() {
		let range = NamespaceDictionaryKey::full_scan(NamespaceId(1025));
		assert!(matches!(range.start, Bound::Included(_) | Bound::Excluded(_)));
		assert!(matches!(range.end, Bound::Included(_) | Bound::Excluded(_)));
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceFlow)]
pub struct NamespaceFlowKey {
	pub namespace: NamespaceId,
	pub flow: FlowId,
}

impl NamespaceFlowKey {
	pub fn encoded(namespace: impl Into<NamespaceId>, flow: impl Into<FlowId>) -> EncodedKey {
		Self {
			namespace: namespace.into(),
			flow: flow.into(),
		}
		.encode()
	}

	pub fn full_scan(namespace_id: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace_id)), Some(Self::link_end(namespace_id)))
	}

	fn link_start(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace_id);
		serializer.to_encoded_key()
	}

	fn link_end(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace_id - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_flow_key_tests {
	use super::NamespaceFlowKey;
	use crate::{
		interface::catalog::{flow::FlowId, id::NamespaceId},
		key::typed::key::Key,
	};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceFlowKey {
			namespace: NamespaceId(0xABCD),
			flow: FlowId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();
		let decoded = NamespaceFlowKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.flow, FlowId(0x123456789ABCDEF0));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceFlowKey {
			namespace: NamespaceId::SYSTEM,
			flow: FlowId(100),
		};
		let key2 = NamespaceFlowKey {
			namespace: NamespaceId::SYSTEM,
			flow: FlowId(200),
		};
		let key3 = NamespaceFlowKey {
			namespace: NamespaceId::DEFAULT,
			flow: FlowId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceHandler)]
pub struct NamespaceHandlerKey {
	pub namespace: NamespaceId,
	pub handler: HandlerId,
}

impl NamespaceHandlerKey {
	pub fn new(namespace: NamespaceId, handler: HandlerId) -> Self {
		Self {
			namespace,
			handler,
		}
	}

	pub fn encoded(namespace: impl Into<NamespaceId>, handler: impl Into<HandlerId>) -> EncodedKey {
		Self::new(namespace.into(), handler.into()).encode()
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace)), Some(Self::link_end(namespace)))
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_handler_key_tests {
	use super::NamespaceHandlerKey;
	use crate::{
		interface::catalog::id::{HandlerId, NamespaceId},
		key::typed::key::Key,
	};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceHandlerKey {
			namespace: NamespaceId(0xABCD),
			handler: HandlerId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();
		let expected: Vec<u8> = vec![
			0xD3, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21,
			0x0F,
		];
		assert_eq!(encoded.as_slice(), expected);

		let decoded = NamespaceHandlerKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.handler, HandlerId(0x123456789ABCDEF0));
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceHandlerKey {
			namespace: NamespaceId::SYSTEM,
			handler: HandlerId(100),
		};
		let key2 = NamespaceHandlerKey {
			namespace: NamespaceId::SYSTEM,
			handler: HandlerId(200),
		};
		let key3 = NamespaceHandlerKey {
			namespace: NamespaceId::DEFAULT,
			handler: HandlerId(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceProcedure)]
pub struct NamespaceProcedureKey {
	pub namespace: NamespaceId,
	pub procedure: ProcedureId,
}

impl NamespaceProcedureKey {
	pub fn encoded(namespace: impl Into<NamespaceId>, procedure: impl Into<ProcedureId>) -> EncodedKey {
		Self {
			namespace: namespace.into(),
			procedure: procedure.into(),
		}
		.encode()
	}

	pub fn full_scan(namespace_id: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace_id)), Some(Self::link_end(namespace_id)))
	}

	fn link_start(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace_id);
		serializer.to_encoded_key()
	}

	fn link_end(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace_id - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_procedure_key_tests {
	use super::NamespaceProcedureKey;
	use crate::{
		interface::catalog::id::{NamespaceId, ProcedureId},
		key::typed::key::Key,
	};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceProcedureKey {
			namespace: NamespaceId(0xABCD),
			procedure: ProcedureId::from_raw(0x123456789ABCDEF0),
		};
		let encoded = key.encode();
		let key = NamespaceProcedureKey::decode(&encoded).unwrap();
		assert_eq!(key.namespace, 0xABCD);
		assert_eq!(key.procedure, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceProcedureKey {
			namespace: NamespaceId::SYSTEM,
			procedure: ProcedureId::persistent(100),
		};
		let key2 = NamespaceProcedureKey {
			namespace: NamespaceId::SYSTEM,
			procedure: ProcedureId::persistent(200),
		};
		let key3 = NamespaceProcedureKey {
			namespace: NamespaceId::DEFAULT,
			procedure: ProcedureId::persistent(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceQueue)]
pub struct NamespaceQueueKey {
	pub namespace: NamespaceId,
	pub queue: QueueId,
}

impl NamespaceQueueKey {
	pub fn new(namespace: NamespaceId, queue: QueueId) -> Self {
		Self {
			namespace,
			queue,
		}
	}

	pub fn encoded(namespace: impl Into<NamespaceId>, queue: impl Into<QueueId>) -> EncodedKey {
		Self::new(namespace.into(), queue.into()).encode()
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace)), Some(Self::link_end(namespace)))
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
mod namespace_queue_key_tests {
	use std::ops::Bound;

	use super::*;

	#[test]
	fn test_encode_decode_roundtrip() {
		// The link row is what makes a queue findable by name; losing either component makes DROP
		// NAMESPACE miss its queues.
		let encoded = NamespaceQueueKey::encoded(NamespaceId(3), QueueId(42));
		let decoded = NamespaceQueueKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(3));
		assert_eq!(decoded.queue, QueueId(42));
	}

	#[test]
	fn test_full_scan_contains_only_the_target_namespace() {
		// Keys are stored bitwise-inverted, so a bound derived with the wrong sign would make DROP
		// NAMESPACE either miss its queues or reach into a sibling.
		let range = NamespaceQueueKey::full_scan(NamespaceId(3));
		let Bound::Included(start) = &range.start else {
			panic!("expected an included start bound")
		};
		let Bound::Included(end) = &range.end else {
			panic!("expected an included end bound")
		};

		assert!(start.as_slice() < end.as_slice(), "the range must be non-empty under byte order");

		for queue in [QueueId(1), QueueId(u64::MAX)] {
			let inside = NamespaceQueueKey::encoded(NamespaceId(3), queue);
			assert!(
				inside.as_slice() >= start.as_slice() && inside.as_slice() <= end.as_slice(),
				"queue {queue:?} in namespace 3 must fall inside the scan range"
			);
		}

		for namespace in [NamespaceId(2), NamespaceId(4)] {
			let neighbour = NamespaceQueueKey::encoded(namespace, QueueId(1));
			assert!(
				neighbour.as_slice() < start.as_slice() || neighbour.as_slice() > end.as_slice(),
				"namespace {namespace:?} must fall outside namespace 3's scan range"
			);
		}
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceRingBuffer)]
pub struct NamespaceRingBufferKey {
	pub namespace: NamespaceId,
	pub ringbuffer: RingBufferId,
}

impl NamespaceRingBufferKey {
	pub fn new(namespace: NamespaceId, ringbuffer: RingBufferId) -> Self {
		Self {
			namespace,
			ringbuffer,
		}
	}

	pub fn encoded(namespace: impl Into<NamespaceId>, ringbuffer: impl Into<RingBufferId>) -> EncodedKey {
		Self::new(namespace.into(), ringbuffer.into()).encode()
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace)), Some(Self::link_end(namespace)))
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_ring_buffer_key_tests {
	use super::NamespaceRingBufferKey;
	use crate::{
		interface::catalog::id::{NamespaceId, RingBufferId},
		key::typed::key::Key,
	};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceRingBufferKey::new(NamespaceId(0xABCD), RingBufferId(0x123456789ABCDEF0));
		let encoded = key.encode();
		let decoded = NamespaceRingBufferKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.ringbuffer, RingBufferId(0x123456789ABCDEF0));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceRingBufferKey::new(NamespaceId::SYSTEM, RingBufferId(100));
		let key2 = NamespaceRingBufferKey::new(NamespaceId::SYSTEM, RingBufferId(200));
		let key3 = NamespaceRingBufferKey::new(NamespaceId::DEFAULT, RingBufferId(0));

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceSeries)]
pub struct NamespaceSeriesKey {
	pub namespace: NamespaceId,
	pub series: SeriesId,
}

impl NamespaceSeriesKey {
	pub fn new(namespace: NamespaceId, series: SeriesId) -> Self {
		Self {
			namespace,
			series,
		}
	}

	pub fn encoded(namespace: impl Into<NamespaceId>, series: impl Into<SeriesId>) -> EncodedKey {
		Self::new(namespace.into(), series.into()).encode()
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace)), Some(Self::link_end(namespace)))
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_series_key_tests {
	use super::NamespaceSeriesKey;
	use crate::{
		interface::catalog::id::{NamespaceId, SeriesId},
		key::typed::key::Key,
	};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceSeriesKey::new(NamespaceId(0xABCD), SeriesId(0x123456789ABCDEF0));
		let encoded = key.encode();
		let decoded = NamespaceSeriesKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.series, SeriesId(0x123456789ABCDEF0));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceSeriesKey::new(NamespaceId::SYSTEM, SeriesId(100));
		let key2 = NamespaceSeriesKey::new(NamespaceId::SYSTEM, SeriesId(200));
		let key3 = NamespaceSeriesKey::new(NamespaceId::DEFAULT, SeriesId(0));

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceSink)]
pub struct NamespaceSinkKey {
	pub namespace: NamespaceId,
	pub sink: SinkId,
}

impl NamespaceSinkKey {
	pub fn encoded(namespace: impl Into<NamespaceId>, sink: impl Into<SinkId>) -> EncodedKey {
		Self {
			namespace: namespace.into(),
			sink: sink.into(),
		}
		.encode()
	}

	pub fn full_scan(namespace_id: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace_id)), Some(Self::link_end(namespace_id)))
	}

	fn link_start(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace_id);
		serializer.to_encoded_key()
	}

	fn link_end(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace_id - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_sink_key_tests {
	use super::NamespaceSinkKey;
	use crate::{
		interface::catalog::id::{NamespaceId, SinkId},
		key::typed::key::Key,
	};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceSinkKey {
			namespace: NamespaceId(0xABCD),
			sink: SinkId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();
		let decoded = NamespaceSinkKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.sink, SinkId(0x123456789ABCDEF0));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceSinkKey {
			namespace: NamespaceId::SYSTEM,
			sink: SinkId(100),
		};
		let key2 = NamespaceSinkKey {
			namespace: NamespaceId::SYSTEM,
			sink: SinkId(200),
		};
		let key3 = NamespaceSinkKey {
			namespace: NamespaceId::DEFAULT,
			sink: SinkId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceSource)]
pub struct NamespaceSourceKey {
	pub namespace: NamespaceId,
	pub source: SourceId,
}

impl NamespaceSourceKey {
	pub fn encoded(namespace: impl Into<NamespaceId>, source: impl Into<SourceId>) -> EncodedKey {
		Self {
			namespace: namespace.into(),
			source: source.into(),
		}
		.encode()
	}

	pub fn full_scan(namespace_id: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace_id)), Some(Self::link_end(namespace_id)))
	}

	fn link_start(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace_id);
		serializer.to_encoded_key()
	}

	fn link_end(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace_id - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_source_key_tests {
	use super::NamespaceSourceKey;
	use crate::{
		interface::catalog::id::{NamespaceId, SourceId},
		key::typed::key::Key,
	};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceSourceKey {
			namespace: NamespaceId(0xABCD),
			source: SourceId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();
		let decoded = NamespaceSourceKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.source, SourceId(0x123456789ABCDEF0));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceSourceKey {
			namespace: NamespaceId::SYSTEM,
			source: SourceId(100),
		};
		let key2 = NamespaceSourceKey {
			namespace: NamespaceId::SYSTEM,
			source: SourceId(200),
		};
		let key3 = NamespaceSourceKey {
			namespace: NamespaceId::DEFAULT,
			source: SourceId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceSumType)]
pub struct NamespaceSumTypeKey {
	pub namespace: NamespaceId,
	pub sumtype: SumTypeId,
}

impl NamespaceSumTypeKey {
	pub fn new(namespace: NamespaceId, sumtype: SumTypeId) -> Self {
		Self {
			namespace,
			sumtype,
		}
	}

	pub fn encoded(namespace: impl Into<NamespaceId>, sumtype: impl Into<SumTypeId>) -> EncodedKey {
		Self::new(namespace.into(), sumtype.into()).encode()
	}

	pub fn full_scan(namespace: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace)), Some(Self::link_end(namespace)))
	}

	fn link_start(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace);
		serializer.to_encoded_key()
	}

	fn link_end(namespace: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_sum_type_key_tests {
	use reifydb_value::value::sumtype::SumTypeId;

	use super::NamespaceSumTypeKey;
	use crate::{interface::catalog::id::NamespaceId, key::typed::key::Key};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceSumTypeKey::new(NamespaceId(0xABCD), SumTypeId(0x123456789ABCDEF0));
		let encoded = key.encode();
		let decoded = NamespaceSumTypeKey::decode(&encoded).unwrap();
		assert_eq!(decoded.namespace, NamespaceId(0xABCD));
		assert_eq!(decoded.sumtype, SumTypeId(0x123456789ABCDEF0));
		assert_eq!(key, decoded);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceSumTypeKey::new(NamespaceId::SYSTEM, SumTypeId(100));
		let key2 = NamespaceSumTypeKey::new(NamespaceId::SYSTEM, SumTypeId(200));
		let key3 = NamespaceSumTypeKey::new(NamespaceId::DEFAULT, SumTypeId(0));

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceTable)]
pub struct NamespaceTableKey {
	pub namespace: NamespaceId,
	pub table: TableId,
}

impl NamespaceTableKey {
	pub fn encoded(namespace: impl Into<NamespaceId>, table: impl Into<TableId>) -> EncodedKey {
		Self {
			namespace: namespace.into(),
			table: table.into(),
		}
		.encode()
	}

	pub fn full_scan(namespace_id: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace_id)), Some(Self::link_end(namespace_id)))
	}

	fn link_start(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace_id);
		serializer.to_encoded_key()
	}

	fn link_end(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace_id - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_table_key_tests {
	use super::NamespaceTableKey;
	use crate::{
		interface::catalog::id::{NamespaceId, TableId},
		key::typed::key::Key,
	};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceTableKey {
			namespace: NamespaceId(0xABCD),
			table: TableId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xFB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21,
			0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = NamespaceTableKey::decode(&encoded).unwrap();
		assert_eq!(key.namespace, 0xABCD);
		assert_eq!(key.table, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceTableKey {
			namespace: NamespaceId::SYSTEM,
			table: TableId(100),
		};
		let key2 = NamespaceTableKey {
			namespace: NamespaceId::SYSTEM,
			table: TableId(200),
		};
		let key3 = NamespaceTableKey {
			namespace: NamespaceId::DEFAULT,
			table: TableId(0),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}

#[derive(Debug, Clone, PartialEq, Key)]
#[key(kind = NamespaceView)]
pub struct NamespaceViewKey {
	pub namespace: NamespaceId,
	pub view: ViewId,
}

impl NamespaceViewKey {
	pub fn encoded(namespace: impl Into<NamespaceId>, view: impl Into<ViewId>) -> EncodedKey {
		Self {
			namespace: namespace.into(),
			view: view.into(),
		}
		.encode()
	}

	pub fn full_scan(namespace_id: NamespaceId) -> EncodedKeyRange {
		EncodedKeyRange::start_end(Some(Self::link_start(namespace_id)), Some(Self::link_end(namespace_id)))
	}

	fn link_start(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(namespace_id);
		serializer.to_encoded_key()
	}

	fn link_end(namespace_id: NamespaceId) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(9);
		serializer.extend_u8(Self::KIND as u8).extend_u64(*namespace_id - 1);
		serializer.to_encoded_key()
	}
}

#[cfg(test)]
pub mod namespace_view_key_tests {
	use super::NamespaceViewKey;
	use crate::{
		interface::catalog::id::{NamespaceId, ViewId},
		key::typed::key::Key,
	};

	#[test]
	fn test_encode_decode() {
		let key = NamespaceViewKey {
			namespace: NamespaceId(0xABCD),
			view: ViewId(0x123456789ABCDEF0),
		};
		let encoded = key.encode();

		let expected: Vec<u8> = vec![
			0xEE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x54, 0x32, 0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21,
			0x0F,
		];

		assert_eq!(encoded.as_slice(), expected);

		let key = NamespaceViewKey::decode(&encoded).unwrap();
		assert_eq!(key.namespace, 0xABCD);
		assert_eq!(key.view, 0x123456789ABCDEF0);
	}

	#[test]
	fn test_order_preserving() {
		let key1 = NamespaceViewKey {
			namespace: NamespaceId::SYSTEM,
			view: ViewId(100),
		};
		let key2 = NamespaceViewKey {
			namespace: NamespaceId::SYSTEM,
			view: ViewId(200),
		};
		let key3 = NamespaceViewKey {
			namespace: NamespaceId::DEFAULT,
			view: ViewId(1),
		};

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();
		let encoded3 = key3.encode();

		assert!(encoded3 < encoded2, "ordering not preserved");
		assert!(encoded2 < encoded1, "ordering not preserved");
	}
}
