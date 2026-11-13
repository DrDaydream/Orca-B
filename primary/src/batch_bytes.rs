//! Preserve bincode's length-plus-bytes layout without visiting each payload byte.
use crypto::Digest;
use serde::{de::Visitor, ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};
use std::{collections::BTreeMap, fmt};

pub(crate) fn serialize<S: Serializer>(
    batches: &BTreeMap<Digest, Vec<u8>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    struct Bytes<'a>(&'a [u8]);
    impl Serialize for Bytes<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_bytes(self.0)
        }
    }
    let mut map = serializer.serialize_map(Some(batches.len()))?;
    for (digest, bytes) in batches {
        map.serialize_entry(digest, &Bytes(bytes))?;
    }
    map.end()
}

pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<Digest, Vec<u8>>, D::Error> {
    struct Bytes(Vec<u8>);
    impl<'de> Deserialize<'de> for Bytes {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct ByteVisitor;
            impl<'de> Visitor<'de> for ByteVisitor {
                type Value = Bytes;
                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str("serialized batch bytes")
                }
                fn visit_byte_buf<E: serde::de::Error>(self, bytes: Vec<u8>) -> Result<Bytes, E> {
                    Ok(Bytes(bytes))
                }
            }
            if deserializer.is_human_readable() {
                Vec::deserialize(deserializer).map(Bytes)
            } else {
                deserializer.deserialize_byte_buf(ByteVisitor)
            }
        }
    }
    BTreeMap::<Digest, Bytes>::deserialize(deserializer).map(|batches| {
        batches
            .into_iter()
            .map(|(digest, bytes)| (digest, bytes.0))
            .collect()
    })
}
