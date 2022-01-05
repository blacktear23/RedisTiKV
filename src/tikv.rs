use redis_module::{ RedisValue };
use tikv_client::{RawClient, Error, Key, Value, KvPair, TransactionClient, Transaction, TransactionOptions, CheckLevel};
use std::collections::{HashMap, LinkedList};
use crate::encoding::*;
use std::sync::{Arc, RwLock, Mutex};
use crate::init::{GLOBAL_CLIENT};


lazy_static! {
    pub static ref PD_ADDRS: Arc<RwLock<Option<Vec<String>>>> = Arc::new(RwLock::new(None));
    pub static ref TIKV_TRANSACTIONS: Arc<RwLock<HashMap<u64, Transaction>>> = Arc::new(RwLock::new(HashMap::new()));
    pub static ref TIKV_TNX_CONN_POOL: Arc<Mutex<LinkedList<TransactionClient>>> = Arc::new(Mutex::new(LinkedList::new()));
}

pub enum TiKVValue {
    Null,
    String(String),
}

impl From<TiKVValue> for RedisValue {
    fn from(item: TiKVValue) -> Self {
        match item {
            TiKVValue::Null => RedisValue::Null,
            TiKVValue::String(s) => RedisValue::BulkString(s),
        }
    }
}

impl From<Vec<u8>> for TiKVValue {
    fn from(item: Vec<u8>) -> Self {
        TiKVValue::String(String::from_utf8_lossy(&item).to_string())
    }
}

fn has_txn(cid: u64) -> bool {
    TIKV_TRANSACTIONS.read().unwrap().contains_key(&cid)
}

fn put_txn(cid: u64, txn: Transaction) {
    TIKV_TRANSACTIONS.write().unwrap().insert(cid, txn);
}

fn get_txn(cid: u64) -> Transaction {
    TIKV_TRANSACTIONS.write().unwrap().remove(&cid).unwrap()
}

async fn get_txn_client() -> Result<TransactionClient, Error> {
    let front = TIKV_TNX_CONN_POOL.lock().unwrap().pop_front();
    if front.is_some() {
        return Ok(front.unwrap());
    }
    let pd_addrs = get_pd_addrs()?;
    let conn = TransactionClient::new(pd_addrs).await?;
    return Ok(conn);
}

fn put_txn_client(client: TransactionClient) {
    TIKV_TNX_CONN_POOL.lock().unwrap().push_back(client);
}

async fn finish_txn(cid: u64, txn: Transaction, in_txn: bool) -> Result<u8, Error> {
    if in_txn {
        put_txn(cid, txn);
        Ok(1)
    } else {
        let mut ntxn = txn;
        let _ = ntxn.commit().await?;
        Ok(1)
    }
}

async fn get_transaction(cid: u64) -> Result<Transaction, Error> {
    if has_txn(cid) {
        let txn = get_txn(cid);
        Ok(txn)
    } else {
        let conn = get_txn_client().await?;
        let txn = conn.begin_with_options(TransactionOptions::default().drop_check(CheckLevel::Warn)).await?;
        put_txn_client(conn);
        Ok(txn)
    }
}

pub fn resp_ok() -> RedisValue {
    RedisValue::SimpleStringStatic("OK")
}

pub fn resp_sstr(val: &'static str) -> RedisValue {
    RedisValue::SimpleStringStatic(val)
}

pub fn get_pd_addrs() -> Result<Vec<String>, Error> {
    let guard = PD_ADDRS.read().unwrap();
    if guard.is_none() {
        return Err(tikv_client::Error::StringError(String::from("TiKV Not connected")))
    }
    Ok(guard.as_ref().unwrap().clone())
}

pub fn get_client() -> Result<Box<RawClient>, Error> {
    let guard = GLOBAL_CLIENT.read().unwrap();
    match guard.as_ref() {
        Some(val) => {
            let client = val.clone();
            Ok(client)
        }
        None => Err(tikv_client::Error::StringError(String::from(
            "TiKV Not connected",
        ))),
    }
}

pub async fn do_async_connect(addrs: Vec<String>) -> Result<RedisValue, Error> {
    let client = RawClient::new(addrs.clone()).await?;
    PD_ADDRS.write().unwrap().replace(addrs.clone());
    GLOBAL_CLIENT.write().unwrap().replace(Box::new(client));
    Ok(resp_ok())
}

pub async fn do_async_begin(cid: u64) -> Result<RedisValue, Error> {
    let _pd_addrs = get_pd_addrs()?;
    if has_txn(cid) {
        return Err(tikv_client::Error::StringError(String::from("Transaction already started")));
    }
    let conn = get_txn_client().await?;
    let txn = conn.begin_with_options(TransactionOptions::default().drop_check(CheckLevel::Warn)).await?;
    put_txn_client(conn);
    put_txn(cid, txn);
    Ok(resp_ok())
}

pub async fn do_async_commit(cid: u64) -> Result<RedisValue, Error> {
    let _ = get_pd_addrs()?;
    if !has_txn(cid) {
        return Err(tikv_client::Error::StringError(String::from("Transaction not started")));
    }
    let mut txn = get_txn(cid);
    txn.commit().await?;
    Ok(resp_ok())
}

pub async fn do_async_rollback(cid: u64) -> Result<RedisValue, Error> {
    let _ = get_pd_addrs()?;
    if !has_txn(cid) {
        return Err(tikv_client::Error::StringError(String::from("Transaction not started")));
    }
    let mut txn = get_txn(cid);
    txn.rollback().await?;
    Ok(resp_ok())
}

pub async fn do_async_get(cid: u64, key: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let value = txn.get(encode_key(DataType::Raw, key)).await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(value.into())
}

pub async fn do_async_hget(cid: u64, key: &str, field: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let value = txn.get(encode_hash_key(key, field)).await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(value.into())
}

pub async fn do_async_get_raw(cid: u64, key: &str) -> Result<Vec<u8>, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let value = txn.get(encode_key(DataType::Raw, key)).await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(value.unwrap())
}

pub async fn do_async_put(cid: u64, key: &str, val: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let _ = txn.put(encode_key(DataType::Raw, key), val.to_owned()).await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_hput(cid: u64, key: &str, field: &str, val: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let _ = txn 
        .put(encode_hash_key(key, field), val.to_owned())
        .await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_lpush(cid: u64, key: &str, elements: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);

    for (pos, e) in elements.iter().enumerate() {
        let _ = txn
            .put(
                encode_list_elem_key(key, l - pos as i64 - 1),
                e.to_owned(),
            )
            .await?;
    }

    txn
        .put(encoded_key, encode_list_meta(l - elements.len() as i64, r))
        .await?;

    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_lrange(cid: u64, key: &str, start: i64, stop: i64) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);

    let num = match stop {
        p if p > 0 => i64::min(stop - start, r - l) + 1,
        n if n < 0 => r + n - l + 1,
        _ => 0,
    };

    let start_pos = l + start;
    let end_pos = start_pos + num;

    let start_key = encode_list_elem_key(key, start_pos);
    let end_key = encode_list_elem_key(key, end_pos);
    let range = start_key..end_key;
    let result = txn.scan(range, num as u32).await?;
    let values: Vec<_> = result
        .into_iter()
        .map(|p| Vec::from(Into::<Vec<u8>>::into(p.value().clone())))
        .collect();

    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_rpush(cid: u64, key: &str, elements: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let encoded_key = encode_list_meta_key(key);
    let (l, r) = decode_list_meta(txn.get(encoded_key.clone()).await?);

    for (pos, e) in elements.iter().enumerate() {
        let _ = txn
            .put(
                encode_list_elem_key(key, r + pos as i64),
                e.to_owned(),
            )
            .await?;
    }

    txn
        .put(encoded_key, encode_list_meta(l, r + elements.len() as i64))
        .await?;

    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_batch_del(cid: u64, keys: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let ekeys = encode_keys(DataType::Raw, keys);
    for i in 0..ekeys.len() {
        let key = ekeys[i].to_owned();
        let _ = txn.delete(key).await?;
    }
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_scan(cid: u64, prefix: &str, limit: u64) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_key(DataType::Raw, prefix)..encode_endkey(DataType::Raw);
    let result = txn.scan(range, limit as u32).await?;
    let values: Vec<_> = result.into_iter().map(|p| Vec::from([
            decode_key(Into::<Vec<u8>>::into(p.key().to_owned())),
            Into::<Vec<u8>>::into(p.value().clone())])).collect();
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_scan_range(cid: u64, start_key: &str, end_key: &str, limit: u64) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_key(DataType::Raw, start_key)..encode_key(DataType::Raw, end_key);
    let result = txn.scan(range, limit as u32).await?;
    let values: Vec<_> = result.into_iter().map(|p| Vec::from([
            decode_key(Into::<Vec<u8>>::into(p.key().to_owned())),
            Into::<Vec<u8>>::into(p.value().to_owned())])).collect();
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_delete_range(cid: u64, start_key: &str, end_key: &str) -> Result<RedisValue, Error> {
    let client = get_client()?;
    let range = encode_key(DataType::Raw, start_key)..encode_key(DataType::Raw, end_key);
    let result = client.delete_range(range).await?;
    Ok(result.into())
}

pub async fn do_async_close() -> Result<RedisValue, Error> {
    let _ = get_client()?;
    *GLOBAL_CLIENT.write().unwrap() = None;
    Ok(resp_sstr("Closed"))
}

async fn wrap_batch_get(txn: &mut Transaction, keys: Vec<String>) -> Result<Vec<KvPair>, Error> {
    let mut ret: Vec<KvPair> = Vec::new();
    for i in 0..keys.len() {
        let key = keys[i].to_owned();
        let val = txn.get(key.clone()).await?;
        match val {
            None => {},
            Some(v) => {
                ret.push(KvPair::new(key, v));
            }
        };
    }
    Ok(ret)
}

pub async fn do_async_batch_get(cid: u64, keys: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let ekeys = encode_keys(DataType::Raw, keys);
    let result = wrap_batch_get(&mut txn, ekeys.clone()).await?;
    let ret: HashMap<Key, Value> = result.into_iter().map(|pair| (pair.0, pair.1)).collect();
    let values: Vec<_> = ekeys.into_iter().map(|k| {
        let data = ret.get(Into::<Key>::into(k).as_ref());
        match data {
            Some(val) => {
                Into::<TiKVValue>::into(val.clone())
            },
            None => {
                TiKVValue::Null
            }
        }
    }).collect();
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_batch_put(cid: u64, kvs: Vec<KvPair>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    for i in 0..kvs.len() {
        let kv = kvs[i].to_owned();
        txn.put(kv.key().to_owned(), kv.value().to_owned()).await?;
    }
    finish_txn(cid, txn, in_txn).await?;
    Ok(resp_ok())
}

pub async fn do_async_exists(cid: u64, keys: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let ekeys = encode_keys(DataType::Raw, keys);
    let result = wrap_batch_get(&mut txn, ekeys).await?;
    let num_items = result.len();
    finish_txn(cid, txn, in_txn).await?;
    Ok(RedisValue::Integer(num_items as i64))
}

pub async fn do_async_hscan(cid: u64, key: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_hash_prefix(key)..encode_hash_prefix_end(key);
    let result = txn.scan(range, 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    let _ = result.into_iter().for_each(|p| {
        values.push(decode_hash_field(Into::<Vec<u8>>::into(p.key().to_owned()), key));
        values.push(Into::<Vec<u8>>::into(p.value().to_owned()));
    });
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_batch_hget(cid: u64, keys: Vec<String>) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let result = wrap_batch_get(&mut txn, keys.clone()).await?;
    let mut kvret: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    result.into_iter().for_each(|p| {
        let key = Into::<Vec<u8>>::into(p.key().to_owned());
        let value = Into::<Vec<u8>>::into(p.value().to_owned());
        kvret.insert(key, value);
    });
    let values: Vec<_> = keys
        .into_iter()
        .map(|k| {
            let data = kvret.get::<Vec<u8>>(&k.into());
            match data {
                Some(val) => Into::<TiKVValue>::into(val.to_owned()),
                None => TiKVValue::Null,
            }
        })
        .collect();
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_hscan_fields(cid: u64, key: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_hash_prefix(key)..encode_hash_prefix_end(key);
    let result = txn.scan(range, 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    result.into_iter().for_each(|p| {
        values.push(decode_hash_field(
            Into::<Vec<u8>>::into(p.key().to_owned()),
            key,
        ));
    });
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_hscan_values(cid: u64, key: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let range = encode_hash_prefix(key)..encode_hash_prefix_end(key);
    let result = txn.scan(range, 10200).await?;
    let mut values: Vec<Vec<u8>> = Vec::new();
    result.into_iter().for_each(|p| {
        values.push(Into::<Vec<u8>>::into(p.value().to_owned()));
    });
    finish_txn(cid, txn, in_txn).await?;
    Ok(values.into())
}

pub async fn do_async_hexists(cid: u64, key: &str, field: &str) -> Result<RedisValue, Error> {
    let in_txn = has_txn(cid);
    let mut txn = get_transaction(cid).await?;
    let result = wrap_batch_get(&mut txn, vec![encode_hash_key(key, field)]).await?;
    finish_txn(cid, txn, in_txn).await?;
    Ok(RedisValue::Integer(result.len() as i64))
}