use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::fmt;

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BulkString(pub Vec<u8>);

impl BulkString {
    pub fn append(&mut self, other: &mut BulkString) {
        self.0.append(&mut other.0);
    }
}

impl fmt::Display for BulkString {
    /// Try to display a friendly string, not a vec of u8
    /// most of the time BulkStrings are a string, but can be used to store binary data
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match String::from_utf8(self.0.to_vec()) {
            Ok(value) => write!(f, "{}", value),
            Err(_) => write!(f, "{:?}", self.0),
        }
    }
}

impl fmt::Debug for BulkString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RespValue {
    SimpleString(String),
    Error(String, Option<String>),
    Integer(i64),
    BulkString(BulkString),
    Array(VecDeque<RespValue>),
    Null,
}

impl RespValue {
    fn to_string(&self) -> Option<String> {
        use RespValue::*;
        match self {
            SimpleString(ref value) => Some(value.clone()),
            BulkString(ref value) => String::from_utf8(value.0.to_vec()).ok(),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        use RespValue::*;
        match *self {
            SimpleString(ref value) => Some(value),
            BulkString(ref value) => std::str::from_utf8(&value.0).ok(),
            _ => None,
        }
    }
}

pub type RedisKey = BulkString;
pub type RedisValue = BulkString;

#[derive(Debug)]
pub enum RedisCmd {
    Ping(Option<RedisValue>),
    Get(RedisKey),
    Delete(Vec<RedisKey>),
    Set(RedisKey, RedisValue),
    Append(RedisKey, RedisValue),
    Keys(RedisValue),
    Exists(RedisKey),
    FlushAll,
    Command,
}

/// Get the next argument from a RespValue::Array
fn get_next_value(resp: &mut VecDeque<RespValue>) -> Result<BulkString> {
    let value = resp
        .pop_front()
        .ok_or_else(|| anyhow::anyhow!("Not enough arguments"))?;
    match value {
        RespValue::BulkString(value) => Ok(value),
        _ => Err(anyhow::anyhow!("Invalid argument, must be BulkString")),
    }
}

impl TryFrom<RespValue> for RedisCmd {
    type Error = anyhow::Error;

    /// Convert RespValues into RedisCmd
    fn try_from(resp: RespValue) -> Result<Self, Self::Error> {
        let mut resp = if let RespValue::Array(resp) = resp {
            resp
        } else {
            bail!("Invalid Command: not an array");
        };

        let cmd = resp
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("No command specified"))?;

        match cmd.to_string().unwrap_or_default().to_uppercase().as_ref() {
            "GET" => Ok(RedisCmd::Get(get_next_value(&mut resp)?)),
            "SET" => Ok(RedisCmd::Set(
                get_next_value(&mut resp).context("Can't get the key of set CMD")?,
                get_next_value(&mut resp).context("Value must be set for set CMD")?,
            )),
            "DEL" => {
                // let mut keys = Vec::with_capacity(resp.len());
                // keys = resp.into();
                Ok(RedisCmd::Delete(
                    resp.drain(..)
                        .map(|key| match key {
                            RespValue::BulkString(key) => key,
                            other => unreachable!("Invalid Type for key: {other:?}"),
                        })
                        .collect(),
                ))
            }
            "APPEND" => Ok(RedisCmd::Append(
                get_next_value(&mut resp).context("Can't get the key of append CMD")?,
                get_next_value(&mut resp).context("Value must be set for append CMD")?,
            )),
            // "CONFIG" => Ok(RedisCmd::Append(
            //     get_next_value(&mut resp).context("Can't get the key of append CMD")?,
            //     get_next_value(&mut resp).context("Value must be set for append CMD")?,
            // )),
            "PING" => Ok(RedisCmd::Ping(get_next_value(&mut resp).ok())),
            "KEYS" => Ok(RedisCmd::Keys(get_next_value(&mut resp)?)),
            "EXISTS" => Ok(RedisCmd::Exists(get_next_value(&mut resp)?)),
            "FLUSHALL" => Ok(RedisCmd::FlushAll),
            "COMMAND" => Ok(RedisCmd::Command),
            "" => Err(anyhow!("No command specified")),
            _ => Err(anyhow!("Invalid Command")),
        }
    }
}
