use crate::tikv::get_instance_id;

pub enum DataType {
    Raw,
    Hash,
    List,
    Set,
}

pub const EMPTY_VALUE: Vec<u8> = vec![];

fn get_prefix(tp: DataType) -> String {
    let dt_prefix = match tp {
        DataType::Raw => "R",
        DataType::Hash => "H",
        DataType::List => "L",
        DataType::Set => "S",
    };
    let uid = get_instance_id().to_be_bytes();
    format!(
        "$R_{}_{}",
        String::from_utf8(uid.to_vec()).unwrap(),
        dt_prefix
    )
}

fn get_rawkv_prefix(tp: DataType) -> String {
    let dt_prefix = match tp {
        DataType::Raw => "R",
        DataType::Hash => "H",
        DataType::List => "L",
        DataType::Set => "S",
    };
    let uid = get_instance_id().to_be_bytes();
    format!(
        "x$R_{}_{}",
        String::from_utf8(uid.to_vec()).unwrap(),
        dt_prefix
    )
}

pub fn encode_rawkv_key(tp: DataType, key: &str) -> String {
    let prefix = get_rawkv_prefix(tp);
    format!("{}_{}", prefix, key)
}

pub fn decode_rawkv_key(key: Vec<u8>) -> Vec<u8> {
    key.clone().drain(15..).collect()
}

pub fn encode_rawkv_keys(tp: DataType, keys: Vec<String>) -> Vec<String> {
    let prefix = get_rawkv_prefix(tp);
    keys.into_iter()
        .map(|val| format!("{}_{}", prefix, val))
        .collect()
}

pub fn encode_rawkv_endkey(tp: DataType) -> String {
    let prefix = get_rawkv_prefix(tp);
    format!("{}`", prefix)
}

pub fn encode_key(tp: DataType, key: &str) -> String {
    let prefix = get_prefix(tp);
    format!("{}_{}", prefix, key)
}

pub fn encode_keys(tp: DataType, keys: Vec<String>) -> Vec<String> {
    let prefix = get_prefix(tp);
    keys.into_iter()
        .map(|val| format!("{}_{}", prefix, val))
        .collect()
}

pub fn encode_endkey(tp: DataType) -> String {
    let prefix = get_prefix(tp);
    format!("{}`", prefix)
}

pub fn decode_key(key: Vec<u8>) -> Vec<u8> {
    key.clone().drain(14..).collect()
}

// two i64 integers (l, r) are used to store the left-bound(inclusive) index and right-bound(exclusive) index of the
// LIST elements stored.
pub fn decode_list_meta(value: Option<Vec<u8>>) -> (i64, i64) {
    match value {
        Some(v) => (
            i64::from_be_bytes(v[0..8].try_into().unwrap()),
            i64::from_be_bytes(v[8..16].try_into().unwrap()),
        ),
        None => (std::u32::MAX as i64, std::u32::MAX as i64),
    }
}

pub fn encode_list_meta(l: i64, r: i64) -> Vec<u8> {
    [l.to_be_bytes(), r.to_be_bytes()].concat().to_vec()
}

pub fn encode_hash_key(key: &str, field: &str) -> String {
    let prefix = get_prefix(DataType::Hash);
    format!("{}_D_{}_{}", prefix, key, field)
}

pub fn encode_hash_prefix(key: &str) -> String {
    let prefix = get_prefix(DataType::Hash);
    format!("{}_D_{}_", prefix, key)
}

pub fn encode_hash_prefix_end(key: &str) -> String {
    let prefix = get_prefix(DataType::Hash);
    format!("{}_D_{}`", prefix, key)
}

pub fn decode_hash_field(rkey: Vec<u8>, key: &str) -> Vec<u8> {
    rkey.clone().drain(16 + key.len() + 1..).collect()
}

pub fn encode_list_meta_key(key: &str) -> String {
    let prefix = get_prefix(DataType::List);
    format!("{}_M_{}", prefix, key)
}

pub fn encode_list_elem_key(key: &str, idx: i64) -> Vec<u8> {
    let prefix = get_prefix(DataType::List);
    let mut res = format!("{}_D_{}_", prefix, key).into_bytes();
    res.append(&mut idx.to_be_bytes().to_vec());
    res
}

pub fn encode_set_key(key: &str, member: &str) -> String {
    let prefix = get_prefix(DataType::Set);
    format!("{}_D_{}_{}", prefix, key, member)
}

pub fn encode_set_key_prefix(key: &str) -> String {
    let prefix = get_prefix(DataType::Set);
    format!("{}_D_{}_", prefix, key)
}

pub fn encode_set_key_prefix_end(key: &str) -> String {
    let prefix = get_prefix(DataType::Set);
    // '~' is greater than '_'.
    format!("{}_D_{}~", prefix, key)
}

pub fn decode_set_member_from_key(key: Vec<u8>) -> Vec<u8> {
    key.clone().drain(18..).collect()
}
