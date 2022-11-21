use std::{
    io::{Read, Write},
};

use bytes::{Buf, BufMut, BytesMut};
use combine::{easy, parser::combinator::AnySendPartialState, stream::PartialStream};
use lunatic::{abstract_process, net::TcpStream, process::ProcessRef, Mailbox, Process};

use anyhow::anyhow;
use lunatic_log::debug;

use crate::{
    encoder::encode,
    storage::{Storage, StorageHandler},
    types::{RedisCmd, RespValue},
};

struct RespReader {
    stream: TcpStream,
    buffer: BytesMut,
    state: AnySendPartialState,
}

impl RespReader {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(1024),
            state: AnySendPartialState::default(),
        }
    }

    fn read(&mut self) -> usize {
        let buffer = &mut [0; 1024];
        let readed = self.stream.read(&mut buffer[..]).unwrap();
        self.buffer.put(&buffer[..readed]);
        readed
    }

    /// Read next Resp messages, a vector is returned because of pipelining
    /// https://redis.io/docs/manual/pipelining/
    fn next(&mut self) -> Option<Vec<RespValue>> {
        if self.buffer.len() == 0 {
            // disconnected
            if self.read() == 0 {
                return None;
            }
        }

        let mut resp_messages = vec![];

        while self.buffer.len() > 0 {
            let (resp, removed_len) = combine::stream::decode(
                crate::parser::resp_parser(),
                &mut easy::Stream(PartialStream(&self.buffer[..])),
                &mut self.state,
            )
            .map_err(|err| {
                let err = err
                    .map_range(|r| {
                        std::str::from_utf8(r)
                            .ok()
                            .map_or_else(|| format!("{:?}", r), |s| s.to_string())
                    })
                    .map_position(|p| p.translate_position(&self.buffer[..]));
                anyhow!(
                    "{}\nIn input: `{}`",
                    err,
                    std::str::from_utf8(&self.buffer).unwrap()
                )
            })
            .unwrap();
            self.buffer.advance(removed_len);

            match resp {
                // If buffer is incomplete, try to read more data
                None if self.buffer.len() > 0 => {
                    // disconnected
                    if self.read() == 0 {
                        return None;
                    }
                }
                Some(value) => resp_messages.push(value),
                None => (),
            }
        }
        Some(resp_messages)
    }
}

pub struct ClientProcess {
    storage: ProcessRef<Storage>,
}

#[abstract_process(visibility = pub)]
impl ClientProcess {
    #[init]
    fn init(this: ProcessRef<Self>, stream: TcpStream) -> Self {
        debug!("Starting client");
        Process::spawn_link(
            (this.clone(), stream),
            |(client, mut stream), _: Mailbox<()>| {
                let mut resp_reader = RespReader::new(stream.clone());
                while let Some(resp_values) = resp_reader.next() {
                    let mut response_buffer = BytesMut::new();
                    for resp_value in resp_values {
                        let response = client.process(resp_value);
                        encode(response, &mut response_buffer);
                    }
                    if response_buffer.len() > 0 {
                        stream.write_all(&response_buffer).unwrap();
                    }
                }
                debug!("Client Disconnected");
            },
        );
        ClientProcess {
            storage: ProcessRef::<Storage>::lookup("storage").unwrap(),
        }
    }

    /// Handle resp messages
    #[handle_request]
    fn process(&mut self, resp: RespValue) -> RespValue {
        let mut cmd: RedisCmd = match resp.try_into() {
            Ok(cmd) => cmd,
            Err(_) => {
                return RespValue::Error("INVALID_COMMAND".into(), None);
            }
        };

        // XXX: create persistence process
        // let mut storage: HashMap<RedisKey, crate::types::RedisValue> = HashMap::new();

        match &mut cmd {
            RedisCmd::Ping(None) => RespValue::SimpleString("PONG".into()),
            RedisCmd::Ping(Some(value)) => RespValue::BulkString(value.clone()),
            RedisCmd::Get(key) => {
                debug!("Getting key: {}", key);
                // let storage = storage.lock();
                if let Some(value) = self.storage.get(key.clone()) {
                    RespValue::BulkString(value.clone())
                } else {
                    RespValue::Null
                }
            }
            RedisCmd::Set(key, value) => {
                debug!("Setting: {}: {}", key, value);
                // storage.lock().insert(key.clone(), value.clone());
                self.storage.set(key.clone(), value.clone());
                RespValue::SimpleString("OK".into())
            }
            RedisCmd::Delete(keys) => {
                debug!("Deleting key: {:?}", keys);
                RespValue::Integer(self.storage.del(keys.clone()))
            }
            RedisCmd::Append(key, value) => {
                debug!("Appending: {}: {}", key, value);
                RespValue::Integer(self.storage.append(key.clone(), value.clone()))
            }
            RedisCmd::Keys(pattern) => {
                debug!("pattern: {}", pattern);
                // TODO: handle patterns
                RespValue::Array(
                    self.storage
                        .keys(pattern.clone())
                        .iter()
                        .map(|k| RespValue::BulkString(k.clone()))
                        .collect(),
                )
            }
            RedisCmd::Exists(key) => {
                debug!("exists: {}", key);
                // TODO: handle patterns
                RespValue::Integer(self.storage.exists(key.clone()))
            }
            RedisCmd::FlushAll => {
                debug!("flush all");
                self.storage.clear();
                RespValue::SimpleString("OK".into())
            }
            // Unimplemented command
            cmd => {
                debug!("Command not implemented: {cmd:?}");
                RespValue::Error("NOT_IMPLEMENTED".into(), None)
            }
        }
    }
}
