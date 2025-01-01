use core::str;

use anyhow::{bail, Result};
use bytes::{Bytes, BytesMut};

use super::handler::RedisValue;

/// TOk represents the start index and last index (exclusive)
/// of the current token in a buffer
#[derive(PartialEq, Clone, Debug)]
pub struct Tok(pub usize, pub usize);

impl<'a> Tok {
    pub fn new(from: usize, to: usize) -> Self {
        Self(from, to)
    }

    pub fn as_slice(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    pub fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

#[derive(PartialEq, Clone, Debug)]
pub enum RESPRaw {
    SimpleString(Tok),
    BulkString(Tok),
    Array(Vec<RESPRaw>),
    // Since the null bulk string has no encoded data, usize represents
    // the position of the next next token
    NullBulkString(usize),
}

/// Return type of the tokenizer, containing the raw token and the start of the next token
#[derive(PartialEq, Clone, Debug)]
pub struct RESPToken(pub RESPRaw, pub usize);

pub fn tokenize(buf: &BytesMut, pos: usize) -> Result<Option<RESPToken>> {
    if pos >= buf.len() {
        return Ok(None);
    }

    match buf[pos] {
        b'+' => parse_basic_string(buf, pos + 1),
        b'$' => parse_bulk_string(buf, pos + 1),
        b'*' => parse_array(buf, pos + 1),
        _ => anyhow::bail!("Identifier '{}' is not valid", buf[pos].to_string()),
    }
}

fn parse_basic_string(buf: &BytesMut, pos: usize) -> Result<Option<RESPToken>> {
    let word = get_next_word(buf, pos);
    Ok(word.map(|(tok, next_post)| RESPToken(RESPRaw::SimpleString(tok), next_post)))
}

fn parse_bulk_string(buf: &BytesMut, pos: usize) -> Result<Option<RESPToken>> {
    match get_next_word(buf, pos) {
        Some((tok, next_pos)) => {
            let len_as_str = str::from_utf8(tok.as_slice(buf))?;
            let expected_len: i32 = len_as_str.parse()?;

            // --- check for null bulk strings
            if expected_len == -1 {
                Ok(Some(RESPToken(RESPRaw::NullBulkString(next_pos), next_pos)))
            } else if expected_len >= 0 {
                let from = next_pos;
                let to = from + expected_len as usize;

                Ok(Some(RESPToken(
                    RESPRaw::BulkString(Tok::new(from, to)),
                    to + 2,
                )))
            } else {
                bail!("Invalid length found for bulk string: {}", expected_len)
            }
        }
        // --- not enough data -> wait for next cycle
        None => Ok(None),
    }
}

fn parse_array(buf: &BytesMut, pos: usize) -> Result<Option<RESPToken>> {
    match get_next_word(buf, pos) {
        Some((tok, next_pos)) => {
            let len_as_str = str::from_utf8(tok.as_slice(buf))?;
            let expected_arr_len: i32 = len_as_str.parse()?;

            match !expected_arr_len.is_negative() {
                true => {
                    // used to keep track of next index in vec to scan
                    let mut cur_pos = next_pos;
                    let mut array: Vec<RESPRaw> = Vec::with_capacity(expected_arr_len as usize);

                    for _ in 0..expected_arr_len {
                        match tokenize(buf, cur_pos)? {
                            Some(parsed_tok) => {
                                cur_pos = parsed_tok.1;
                                array.push(parsed_tok.0);
                            }
                            None => return Ok(None),
                        }
                    }

                    Ok(Some(RESPToken(RESPRaw::Array(array), cur_pos)))
                }
                false => bail!("Invalid array length: {}", expected_arr_len),
            }
        }
        None => Ok(None),
    }
}

/// Returns the range of the next word
pub fn get_next_word(buf: &BytesMut, pos: usize) -> Option<(Tok, usize)> {
    // --- end of buffer
    if pos >= buf.len() {
        return None;
    }

    // --- find first occurence of "\r\n"
    let next_crlf = buf[pos..].windows(2).position(|w| w == b"\r\n");
    next_crlf.map(|cr| (Tok::new(pos, pos + cr), pos + cr + 2))
}

impl RedisValue {
    pub fn serialize(self) -> Result<String> {
        match self {
            RedisValue::SimpleString(s) => Ok(format!("+{}\r\n", str::from_utf8(&s)?)),
            RedisValue::BulkString(b) => Ok(format!("${}\r\n{}\r\n", b.len(), str::from_utf8(&b)?)),
            RedisValue::NullBulkString => Ok(String::from("$-1\r\n")),
            RedisValue::SimpleError(e) => Ok(format!("-{}\r\n", str::from_utf8(&e)?)),
            RedisValue::Array(arr) => Ok(format!(
                "*{}\r\n{}",
                arr.len(),
                arr.into_iter()
                    .map(|m| m.serialize().unwrap())
                    .collect::<Vec<String>>()
                    .join("")
            )),
        }
    }
}
