use bytes::{BytesMut, BufMut};

use crate::types::{RespValue, BulkString};

pub fn encode_string(prefix: u8, value: String, buf: &mut BytesMut) {
    buf.reserve(value.len() + 3);
    buf.put_u8(prefix);
    buf.put(&value.into_bytes()[..]);
    buf.put(&b"\r\n"[..]);
}

// Encode a RespValue as bytes
pub fn encode(resp: RespValue, buf: &mut BytesMut) {
    match resp {
        RespValue::Null => {
            buf.reserve(5);
            buf.put(&b"$-1\r\n"[..]);
        }
        RespValue::SimpleString(value) => encode_string(b'+', value, buf),
        // TODO: support description
        RespValue::Error(value, _description) => encode_string(b'-', value, buf),
        RespValue::Integer(value) => encode_string(b':', value.to_string(), buf),
        RespValue::BulkString(BulkString(value)) => {
            let len_str = value.len().to_string();
            buf.reserve(value.len() + len_str.len() + 5);
            buf.put_u8(b'$');
            buf.put(&len_str.into_bytes()[..]);
            buf.put(&b"\r\n"[..]);
            buf.put(&value[..]);
            buf.put(&b"\r\n"[..]);
        }
        RespValue::Array(mut values) => {
            let len_str = values.len().to_string();
            buf.reserve(values.len() * 2 + len_str.len());
            buf.put_u8(b'*');
            buf.put(&len_str.into_bytes()[..]);
            buf.put(&b"\r\n"[..]);
            values.drain(..).for_each(|value| {
                encode(value, buf);
            });
        }
    }
}
