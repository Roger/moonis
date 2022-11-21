use std::collections::HashMap;

use lunatic::{abstract_process, process::ProcessRef};

use crate::types::{BulkString, RedisKey, RedisValue};

#[derive(Default)]
pub struct Storage {
    store: HashMap<RedisKey, RedisValue>,
}

#[abstract_process(visibility = pub)]
impl Storage {
    #[init]
    fn init(_: ProcessRef<Self>, _: ()) -> Self {
        Self::default()
    }

    #[handle_request]
    fn get(&mut self, key: RedisKey) -> Option<RedisValue> {
        self.store.get(&key).cloned()
    }

    #[handle_request]
    fn set(&mut self, key: RedisKey, value: RedisValue) -> bool {
        self.store.insert(key, value).is_some()
    }

    #[handle_request]
    fn del(&mut self, keys: Vec<RedisKey>) -> i64 {
        let mut removed = 0;
        for key in keys {
            if self.store.remove(&key).is_some() {
                removed += 1;
            }
        }
        removed
    }

    #[handle_request]
    fn append(&mut self, key: RedisKey, mut value: BulkString) -> i64 {
        let current_value = self
            .store
            .entry(key.clone())
            .or_insert_with(|| BulkString("".into()));
        current_value.append(&mut value);
        current_value.0.len() as i64
    }

    #[handle_request]
    fn keys(&mut self, _key: RedisKey) -> Vec<RedisKey> {
        // TODO: handle patterns
        self.store.keys().cloned().collect()
    }

    #[handle_request]
    fn exists(&mut self, key: RedisKey) -> i64 {
        self.store.contains_key(&key).into()
    }

    #[handle_request]
    fn clear(&mut self) {
        self.store.clear()
    }
}
