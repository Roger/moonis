use combine::{
    count_min_max,
    error::{ParseError, StreamError},
    parser::{
        byte::{byte, take_until_bytes},
        choice::choice,
        combinator::{any_send_partial_state, AnySendPartialState},
        range::{range, recognize, take},
    },
    stream::{RangeStream, StreamErrorFor},
    value, Parser,
};

use crate::types::{BulkString, RespValue};

/// Line parser for resp protocol, reads until `\r\n`
fn line<'a, Input>() -> impl Parser<Input, Output = &'a str, PartialState = AnySendPartialState> + 'a
where
    Input: RangeStream<Token = u8, Range = &'a [u8]> + 'a,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    any_send_partial_state(
        recognize(take_until_bytes(&b"\r\n"[..]).with(take(2).map(|_| ()))).and_then(
            |line: &[u8]| {
                std::str::from_utf8(&line[..line.len() - 2]).map_err(StreamErrorFor::<Input>::other)
            },
        ),
    )
}

/// Integer parser (i64) for resp protocol
/// ie. :42\r\n
fn integer<'a, Input>() -> impl Parser<Input, Output = i64, PartialState = AnySendPartialState> + 'a
where
    Input: RangeStream<Token = u8, Range = &'a [u8]> + 'a,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    any_send_partial_state(line().and_then(|line| match line.trim().parse() {
        Ok(value) => Ok(value),
        Err(_) => Err(StreamErrorFor::<Input>::message_static_message(
            "Invalid Integer",
        )),
    }))
}

/// Resp2 parser for server commands
/// clients send only command as SimpleString (simple commands easy to send from telnet/netcat) or
/// using Array of BulkStrings with the first element as the command
/// That's why we only parse a subset of the resp2 protocol here, we only need to encode the rest
/// of the spec to create anwsers to the clients
pub fn resp_parser<'a, Input>(
) -> impl Parser<Input, Output = RespValue, PartialState = AnySendPartialState> + 'a
where
    Input: RangeStream<Token = u8, Range = &'a [u8]> + 'a,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    // Simple command parser, this is just a string with args splited by whitespace
    // ie. GET key
    let simple_command = || {
        line().map(|line| {
            let values = line
                .split_whitespace()
                .map(|part| RespValue::BulkString(BulkString(part.into())))
                .collect();
            RespValue::Array(values)
        })
    };

    // Binary friendly string
    let bulk = || {
        integer().then_partial(move |&mut length| {
            if length < 0 {
                value(RespValue::Null).left()
            } else {
                take(length as usize)
                    .map(|data: &[u8]| RespValue::BulkString(BulkString(data.into())))
                    .skip(range(&b"\r\n"[..]))
                    .right()
            }
        })
    };

    // Array of bulk strings
    let array = || {
        integer().then_partial(move |&mut length| {
            if length < 0 {
                value(RespValue::Null).left()
            } else {
                let length = length as usize;
                count_min_max(length, length, byte(b'$').with(bulk()))
                    .map(|mut results: Vec<_>| {
                        // We should never hit an Err result here, because the parsing should fail
                        // before, count_min_max should get less values if a resp value fails and
                        // raising an all errors that happened
                        let results = results.drain(..).map(|x| x).collect();
                        RespValue::Array(results)
                    })
                    .right()
            }
        })
    };

    any_send_partial_state(choice((byte(b'*').with(array()), simple_command())))
}
