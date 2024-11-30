use core::panic;
use std::{
    fmt::Debug,
    io::{self, Read, Result},
};

use crate::api::{Encoder, Parser};

pub fn parse_bool(reader: &mut impl Read) -> Result<bool> {
    let mut buf = [0];
    reader.read_exact(&mut buf)?;

    if buf[0] == 1 {
        Ok(true)
    } else {
        Ok(false)
    }
}

pub fn encode_bool(value: bool) -> Vec<u8> {
    let mut buf = [0];
    if value {
        buf[0] = 1;
    }

    buf.to_vec()
}

pub fn parse_int8(reader: &mut impl Read) -> Result<i8> {
    let mut buf = [0];
    reader.read_exact(&mut buf)?;
    Ok(i8::from_be_bytes(buf))
}

impl Encoder for i8 {
    fn encode(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

pub fn parse_int16(reader: &mut impl Read) -> Result<i16> {
    let mut buf = [0; 2];
    reader.read_exact(&mut buf)?;
    Ok(i16::from_be_bytes(buf))
}

impl Encoder for i16 {
    fn encode(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl Parser<i32> for i32 {
    fn parse(reader: &mut impl Read) -> Result<i32> {
        Ok(parse_int32(reader)?)
    }
}

pub fn parse_int32(reader: &mut impl Read) -> Result<i32> {
    let mut buf = [0; 4];
    reader.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

impl Encoder for i32 {
    fn encode(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

pub fn parse_int64(reader: &mut impl Read) -> Result<i64> {
    let mut buf = [0; 8];
    reader.read_exact(&mut buf)?;
    Ok(i64::from_be_bytes(buf))
}

pub fn parse_varint(buf: &mut impl Read) -> Result<i32> {
    let num = parse_unsigned_varlong(buf)?;
    Ok(num as i32)
}

pub fn parse_unsigned_varint(buf: &mut impl Read) -> Result<u32> {
    let num = parse_unsigned_varlong(buf)?;
    Ok(num as u32)
}

pub fn parse_unsigned_varlong(buf: &mut impl Read) -> Result<u64> {
    let mut length: u8 = 0;
    let mut bytes = vec![];

    for b in buf.bytes() {
        let byte = b?;
        bytes.push(byte);
        length += 1;

        if (byte & 0x80) == 0 {
            break;
        }
        if length == 10 {
            panic!("Invalid varint");
        }
    }

    bytes.reverse();

    let mut value: u64 = 0;
    for byte in bytes {
        value <<= 7;
        value += (byte & 0x3f) as u64;
    }

    Ok(value)
}

pub fn encode_varint(mut varint: u64) -> Vec<u8> {
    let mut buf = Vec::new();

    if varint == 0 {
        buf.push(0);
        return buf;
    }

    while varint != 0 {
        let byte = (varint & 0x3f) as u8;
        buf.push(byte | 0x80);
        varint >>= 7;
    }

    // clear msb in last byte
    let length = buf.len();
    buf[length - 1] &= 0x3f;

    buf
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct Uuid {
    pub uuid: [u8; 16],
}

impl Uuid {
    pub fn new() -> Self {
        Uuid { uuid: [0; 16] }
    }
}

impl Parser<Self> for Uuid {
    fn parse(reader: &mut impl Read) -> Result<Self> {
        let mut buf = [0; 16];
        reader.read_exact(&mut buf)?;
        Ok(Uuid { uuid: buf })
    }
}

impl Encoder for Uuid {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.uuid);
        buf
    }
}

#[derive(Debug)]
pub struct CompactString(pub String);

impl Parser<Self> for CompactString {
    fn parse(reader: &mut impl Read) -> Result<Self> {
        Ok(CompactString(parse_compact_string(reader)?))
    }
}

pub fn parse_compact_string(buf: &mut impl Read) -> Result<String> {
    let length = parse_unsigned_varlong(buf)? as usize;
    let mut string = vec![0u8; length - 1];
    buf.read_exact(&mut string)?;

    Ok(String::from_utf8(string).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?)
}

pub fn encode_compact_string(string: String) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend(encode_varint(buf.len() as u64 + 1));
    buf.extend(string.bytes());
    buf
}

pub fn parse_nullable_string(reader: &mut impl Read) -> Result<String> {
    let mut buf = [0; 2];
    reader.read_exact(&mut buf)?;
    let length = i16::from_be_bytes(buf);

    if length == -1 {
        return Ok(String::new());
    }

    let mut string = vec![0u8; length as usize];
    reader.read_exact(&mut string)?;

    Ok(String::from_utf8(string).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?)
}

pub fn encode_compact_nullable_string(string: Option<String>) -> Vec<u8> {
    let mut buf = Vec::new();

    match string {
        Some(s) => {
            let length = s.len() as u64;
            buf.extend(encode_varint(length + 1));
            buf.extend(s.bytes());
        }
        None => {
            buf.extend(0u8.to_be_bytes());
        }
    }

    buf
}

pub fn parse_compact_array_with_tag_buffer<P, R>(reader: &mut R) -> Result<Vec<P>>
where
    P: Parser<P>,
    R: Read,
{
    let length = parse_unsigned_varlong(reader)?;
    let mut array = Vec::new();

    for _ in 0..length - 1 {
        let item = P::parse(reader)?;
        array.push(item);
        parse_tag_buffer(reader)?;
    }

    Ok(array)
}

pub fn parse_compact_array<P, R>(reader: &mut R) -> Result<Vec<P>>
where
    P: Parser<P> + Debug,
    R: Read,
{
    let length = parse_unsigned_varlong(reader)?;
    let mut array = Vec::new();

    for _ in 0..length - 1 {
        let item = P::parse(reader)?;
        array.push(item);
    }

    Ok(array)
}

pub fn encode_compact_array<T: Encoder>(array: Vec<T>) -> Vec<u8> {
    let mut res = Vec::new();

    if array.is_empty() {
        res.extend(encode_varint(0));
    } else {
        res.extend(encode_varint(array.len() as u64 + 1));
    }

    for item in array {
        res.extend(item.encode());
    }

    res
}

pub fn parse_nullable_field<P, R>(reader: &mut R) -> Result<Option<P>>
where
    P: Parser<P>,
    R: Read,
{
    let mut buf = [0; 1];
    reader.read_exact(&mut buf)?;
    let length = buf[0] as i8;

    if length == -1 {
        return Ok(None);
    }

    Ok(Some(P::parse(reader)?))
}

pub fn encode_nullable_field<T: Encoder>(array: Option<T>) -> Vec<u8> {
    let mut buf = Vec::new();

    match array {
        Some(f) => {
            buf.extend(f.encode());
        }
        None => {
            buf.extend((-1i8).encode());
        }
    }

    buf
}

// ignoring tag buffers for now
pub fn parse_tag_buffer(reader: &mut impl Read) -> Result<Vec<u8>> {
    let mut buf = [0];
    reader.read_exact(&mut buf)?;
    Ok(Vec::new())
}

pub fn encode_tag_buffer() -> Vec<u8> {
    vec![0]
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::primitives::{parse_compact_string, parse_unsigned_varlong};

    #[test]
    fn test_decode_single_byte_varint() {
        let mut cursor = Cursor::new(&[10]);
        let value = parse_unsigned_varlong(&mut cursor).unwrap();
        assert_eq!(10, value);
    }

    #[test]
    fn test_decode_multi_byte_variant() {
        let mut cursor = Cursor::new(&[0x96, 0x1]);
        let value = parse_unsigned_varlong(&mut cursor).unwrap();
        assert_eq!(150, value);

        let mut cursor = Cursor::new(&[0x80, 0x80, 0x01]);
        let value = parse_unsigned_varlong(&mut cursor).unwrap();
        assert_eq!(16384, value);
    }

    #[test]
    fn test_decode_compact_string() {
        let string = "test";
        let mut buf: Vec<u8> = Vec::new();
        buf.push(string.len() as u8 + 1);
        buf.extend(string.as_bytes());
        let mut cursor = Cursor::new(&buf);
        assert_eq!("test", &parse_compact_string(&mut cursor).unwrap());
    }
}
