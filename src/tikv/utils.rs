use redis_module::{ RedisValue };
use tikv_client::{Error, KvPair, TransactionClient, Transaction, TransactionOptions, CheckLevel, RetryOptions, Backoff};
use crate::tikv::{PD_ADDRS, TIKV_TRANSACTIONS, TIKV_TNX_CONN_POOL};
use crate::utils::sleep;

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

pub fn has_txn(cid: u64) -> bool {
    TIKV_TRANSACTIONS.read().unwrap().contains_key(&cid)
}

pub fn put_txn(cid: u64, txn: Transaction) {
    TIKV_TRANSACTIONS.write().unwrap().insert(cid, txn);
}

pub fn get_txn(cid: u64) -> Transaction {
    TIKV_TRANSACTIONS.write().unwrap().remove(&cid).unwrap()
}

pub async fn get_txn_client() -> Result<TransactionClient, Error> {
    let front = TIKV_TNX_CONN_POOL.lock().unwrap().pop_front();
    if front.is_some() {
        return Ok(front.unwrap());
    }
    let pd_addrs = get_pd_addrs()?;
    let conn = TransactionClient::new(pd_addrs).await?;
    return Ok(conn);
}

pub fn put_txn_client(client: TransactionClient) {
    if PD_ADDRS.read().unwrap().is_none() {
        drop(client);
        return;
    }
    TIKV_TNX_CONN_POOL.lock().unwrap().push_back(client);
}

pub async fn finish_txn(cid: u64, txn: Transaction, in_txn: bool) -> Result<u8, Error> {
    if in_txn {
        put_txn(cid, txn);
        Ok(1)
    } else {
        let mut ntxn = txn;
        let _ = ntxn.commit().await?;
        Ok(1)
    }
}

pub fn get_transaction_option() -> TransactionOptions {
    let opts = TransactionOptions::new_pessimistic();
    let mut retry_opts = RetryOptions::default_pessimistic();
    retry_opts.lock_backoff = Backoff::full_jitter_backoff(2, 500, 10);
    opts.drop_check(CheckLevel::Warn)
        .use_async_commit()
        .try_one_pc()
        .retry_options(retry_opts)
}

pub async fn get_transaction(cid: u64) -> Result<Transaction, Error> {
    if has_txn(cid) {
        let txn = get_txn(cid);
        Ok(txn)
    } else {
        let conn = get_txn_client().await?;
        let txn = conn.begin_with_options(get_transaction_option()).await?;
        put_txn_client(conn);
        Ok(txn)
    }
}

pub fn get_pd_addrs() -> Result<Vec<String>, Error> {
    let guard = PD_ADDRS.read().unwrap();
    if guard.is_none() {
        return Err(tikv_client::Error::StringError(String::from("TiKV Not connected")))
    }
    Ok(guard.as_ref().unwrap().clone())
}

pub async fn wrap_batch_get(txn: &mut Transaction, keys: Vec<String>) -> Result<Vec<KvPair>, Error> {
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

pub async fn wrap_put(txn: &mut Transaction, key: &str, value: &str) -> Result<(), Error> {
    let mut last_err: Option<Error> = None;
    for i in 0..1000 {
        match txn.put(key.to_owned(), value.to_owned()).await {
            Ok(_) => {
                return Ok(());
            },
            Err(err) => {
                if let Error::KeyError(ref kerr) = err {
                    if kerr.retryable != "" {
                        // do retry
                        last_err.replace(err);
                        sleep(std::cmp::min(2 * i, 500)).await;
                        continue;
                    }
                }
                // Cannot retry
                return Err(err);
            }
        }
    }
    match last_err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}