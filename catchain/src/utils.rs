extern crate hex;

/// Imports
pub use super::*;
use crate::ton_api::IntoBoxed;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/*
    to string conversions
*/

pub fn bytes_to_string(v: &::ton_api::ton::bytes) -> String {
    hex::encode(&v.0)
}

pub fn public_key_hashes_to_string(v: &[PublicKeyHash]) -> String {
    let mut result: String = "[".to_string();
    let mut first = true;

    for key in v {
        if !first {
            result += ", ";
        } else {
            first = false;
        }

        result = format!("{}{}", result, key);
    }

    result + "]"
}

pub fn time_to_string(time: &std::time::SystemTime) -> String {
    let datetime: chrono::DateTime<chrono::offset::Utc> = time.clone().into();

    datetime.format("%Y-%m-%d %T.%f").to_string()
}

/*
    type conversions
*/

pub fn parse_hex(hex_asm: &str) -> Vec<u8> {
    let mut hex_bytes = hex_asm
        .as_bytes()
        .iter()
        .filter_map(|b| match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        })
        .fuse();

    let mut bytes = Vec::new();
    while let (Some(h), Some(l)) = (hex_bytes.next(), hex_bytes.next()) {
        bytes.push(h << 4 | l)
    }

    bytes
}

pub fn parse_hex_to_array(hex_asm: &str, dst: &mut [u8]) {
    assert!(dst.len() * 2 >= hex_asm.len());
    dst.iter_mut().for_each(|x| *x = 0);

    for (i, c) in hex_asm.chars().enumerate() {
        dst[i / 2] = dst[i / 2] * 16 + u8::from_str_radix(&c.to_string(), 16).unwrap();
    }
}

pub fn parse_hex_as_int256(hex_asm: &str) -> UInt256 {
    let hex_bytes = parse_hex(&hex_asm);
    let array = [0; 32];
    let bytes = &hex_bytes[..array.len()];
    UInt256::from(bytes)
}

pub fn parse_hex_as_bytes(hex_asm: &str) -> ::ton_api::ton::bytes {
    ::ton_api::ton::bytes(parse_hex(&hex_asm))
}

pub fn parse_hex_as_public_key(hex_asm: &str) -> PublicKey {
    assert!(hex_asm.len() % 2 == 0);
    let mut key_slice = vec![0; hex_asm.len() / 2];
    parse_hex_to_array(hex_asm, &mut key_slice[..]);
    //TODO: errors processing for key creation
    Arc::new(adnl::common::KeyOption::from_tl_serialized_public_key(&key_slice).unwrap())
}

pub fn parse_hex_as_public_key_hash(hex_asm: &str) -> PublicKeyHash {
    let mut key_slice: [u8; 32] = [0; 32];
    parse_hex_to_array(hex_asm, &mut key_slice);
    adnl::common::KeyId::from_data(key_slice)
}

pub fn parse_hex_as_session_id(hex_asm: &str) -> SessionId {
    parse_hex_as_int256(hex_asm)
}

pub fn parse_hex_as_private_key(hex_asm: &str) -> PrivateKey {
    assert!(hex_asm.len() % 2 == 0);
    let mut key_slice = vec![0; hex_asm.len() / 2];
    parse_hex_to_array(hex_asm, &mut key_slice[..]);
    //TODO: errors processing for key creation
    assert!(key_slice.len() == 32);
    let private_key = ed25519_dalek::SecretKey::from_bytes(&key_slice[..32]).unwrap();
    Arc::new(adnl::common::KeyOption::from_ed25519_secret_key(
        private_key,
    ))
}

pub fn get_hash(data: &::ton_api::ton::bytes) -> BlockHash {
    let mut hasher = Sha256::new();
    hasher.input(&data.0);
    let result: &[u8] = &hasher.result();
    UInt256::from(result)
}

pub fn int256_to_public_key_hash(public_key: &::ton_api::ton::int256) -> PublicKeyHash {
    adnl::common::KeyId::from_data(public_key.0)
}

pub fn get_public_key_hash(public_key: &PublicKey) -> PublicKeyHash {
    public_key.id().clone()
}

pub fn public_key_hash_to_int256(v: &PublicKeyHash) -> ::ton_api::ton::int256 {
    ::ton_api::ton::int256(*v.data())
}

pub fn get_overlay_id(first_block: &ton_api::ton::catchain::FirstBlock) -> Result<SessionId> {
    let serialized_first_block = serialize_tl_boxed_object!(first_block);
    let overlay_id = ::ton_api::ton::pub_::publickey::Overlay {
        name: serialized_first_block.into(),
    };
    let serialized_overlay_id = serialize_tl_boxed_object!(&overlay_id.into_boxed());
    let mut hasher = Sha256::new();
    hasher.input(&serialized_overlay_id.0);
    let hash: [u8; 32] = hasher.result().clone().into();
    Ok(hash.into())
}

pub fn get_block_id(
    incarnation: &SessionId,
    source_hash: &PublicKeyHash,
    height: ::ton_api::ton::int,
    payload: &BlockPayload,
) -> ton::BlockId {
    let data_hash = get_hash(payload);

    ::ton_api::ton::catchain::block::Id::Catchain_Block_Id(Box::new(
        ::ton_api::ton::catchain::block::id::Id {
            incarnation: incarnation.clone().into(),
            src: public_key_hash_to_int256(source_hash),
            height: height,
            data_hash: data_hash.into(),
        },
    ))
}

pub fn get_block_dependency_id(block: &ton::BlockDep, receiver: &dyn Receiver) -> ton::BlockId {
    ::ton_api::ton::catchain::block::Id::Catchain_Block_Id(Box::new(
        ::ton_api::ton::catchain::block::id::Id {
            incarnation: receiver.get_incarnation().clone().into(),
            src: public_key_hash_to_int256(receiver.get_source_public_key_hash(block.src as usize)),
            height: block.height,
            data_hash: block.data_hash,
        },
    ))
}

pub fn get_root_block_id(incarnation: &SessionId) -> ton::BlockId {
    ::ton_api::ton::catchain::block::Id::Catchain_Block_Id(Box::new(
        ::ton_api::ton::catchain::block::id::Id {
            incarnation: incarnation.clone().into(),
            src: incarnation.clone().into(),
            height: 0,
            data_hash: incarnation.clone().into(),
        },
    ))
}

pub fn get_block_id_hash(id: &ton::BlockId) -> BlockHash {
    let mut serial = Vec::<u8>::new();
    let mut serializer = ton_api::Serializer::new(&mut serial);
    serializer.write_boxed(id).unwrap();
    get_hash(&ton_api::ton::bytes(serial))
}

pub fn get_block_dependency_hash(block: &ton::BlockDep, receiver: &dyn Receiver) -> BlockHash {
    get_block_id_hash(&get_block_dependency_id(block, receiver))
}

/*
    serialization utils
*/

#[macro_export]
macro_rules! serialize_tl_bare_object
{
  ($($args:expr),*) => {{
    let mut ret : BlockPayload = BlockPayload::default();
    let mut serializer = ton_api::Serializer::new(&mut ret.0);

    $(serializer.write_bare($args).unwrap();)*

    ret
  }}
}

#[macro_export]
macro_rules! serialize_tl_boxed_object
{
  ($($args:expr),*) => {{
    let mut ret : BlockPayload = BlockPayload::default();
    let mut serializer = ton_api::Serializer::new(&mut ret.0);

    $(serializer.write_boxed($args).unwrap();)*

    ret
  }}
}

pub fn serialize_block_with_payload(
    block: &ton::Block,
    payload: &BlockPayload,
) -> Result<BlockPayload> {
    let mut raw_data: BlockPayload = BlockPayload::default();
    let mut serializer = ton_api::Serializer::new(&mut raw_data.0);

    serializer.write_boxed(&block.clone().into_boxed())?;
    serializer.write_bare(payload)?;

    Ok(raw_data)
}

pub fn serialize_query_boxed_response<T>(response: Result<T>) -> Result<BlockPayload>
where
    T: ::ton_api::BoxedSerialize,
{
    match response {
        Ok(response) => {
            let mut ret: BlockPayload = BlockPayload::default();
            let mut serializer = ton_api::Serializer::new(&mut ret.0);

            serializer.write_boxed(&response).unwrap();

            Ok(ret)
        }
        Err(err) => Err(err),
    }
}

pub fn serialize_query_bare_response<T>(response: Result<T>) -> Result<BlockPayload>
where
    T: ::ton_api::BareSerialize,
{
    match response {
        Ok(response) => Ok(serialize_tl_bare_object!(&response)),
        Err(err) => Err(err),
    }
}

pub fn deserialize_tl_bare_object<T: ::ton_api::BareDeserialize>(
    bytes: &BlockPayload,
) -> Result<T> {
    let cloned_bytes = bytes.clone();
    let data: &mut &[u8] = &mut cloned_bytes.0.as_ref();
    ton_api::Deserializer::new(data).read_bare()
}

pub fn deserialize_tl_boxed_object<T: ::ton_api::BoxedDeserialize>(
    bytes: &BlockPayload,
) -> Result<T> {
    let cloned_bytes = bytes.clone();
    let data: &mut &[u8] = &mut cloned_bytes.0.as_ref();
    ton_api::Deserializer::new(data).read_boxed()
}

/*
   metrics
*/

pub fn instance_counter_to_string(
    basic_key: &String,
    metrics: &BTreeMap<String, &metrics_runtime::Measurement>,
) -> String {
    let create_key = basic_key.clone() + ".create";
    let create_value = metrics.get(&create_key);
    let create_value = match create_value {
        Some(metrics_runtime::Measurement::Counter(value)) => value,
        _ => return "N/A".to_string(),
    };

    let drop_key = basic_key.clone() + ".drop";
    let drop_value = metrics.get(&drop_key);
    let drop_value = match drop_value {
        Some(metrics_runtime::Measurement::Counter(value)) => value,
        _ => return "N/A".to_string(),
    };

    let instance_count = create_value - drop_value;

    if *drop_value == 0 && instance_count == *create_value {
        format!("{}", instance_count)
    } else {
        format!("{} ({}-{})", instance_count, create_value, drop_value)
    }
}

pub fn dump_metric(
    key: &String,
    value: &metrics_runtime::Measurement,
    metrics: &BTreeMap<String, &metrics_runtime::Measurement>,
) {
    if !key.contains(".create") && !key.contains(".drop") {
        debug!("...{}={:?}", key, value);
        return;
    }

    let create_key = key;

    if !create_key.contains(".create") {
        return;
    }

    let basic_key = create_key.replace(".create", "");

    debug!(
        "...{}={}",
        basic_key,
        instance_counter_to_string(&basic_key, metrics)
    );
}

pub fn dump_metrics(
    metrics_receiver: &metrics_runtime::Receiver,
    dump: &dyn Fn(
        &String,
        &metrics_runtime::Measurement,
        &BTreeMap<String, &metrics_runtime::Measurement>,
    ),
) {
    let metrics = metrics_receiver.controller().snapshot().into_measurements();
    let map: BTreeMap<String, &metrics_runtime::Measurement> = metrics
        .iter()
        .map(|x| (x.0.name().to_string(), &x.1))
        .collect();

    for (key, value) in &map {
        dump(key, value, &map);
    }
}
