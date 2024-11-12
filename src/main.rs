#![allow(unused_imports)]
use core::panic;
use std::{
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

enum ApiVersionsError {
    NoError = 0,
    UnsupportedVersion = 35,
}

struct ApiVersionsV4Response {
    error_code: i16,
    api_keys: Vec<ApiKeys>,
    throttle_time_ms: i32,
}

impl ApiVersionsV4Response {
    fn encode(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::new();
        let api_keys_len: u8 = (self.api_keys.len() + 1).try_into().unwrap();

        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        buffer.extend_from_slice(&api_keys_len.to_be_bytes());
        for api_key in self.api_keys {
            buffer.extend_from_slice(&api_key.encode());
        }
        buffer.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        buffer.extend_from_slice(&0i8.to_be_bytes());

        buffer
    }
}

struct ApiKeys {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

impl ApiKeys {
    fn encode(self) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.extend_from_slice(&self.api_key.to_be_bytes());
        buffer.extend_from_slice(&self.min_version.to_be_bytes());
        buffer.extend_from_slice(&self.max_version.to_be_bytes());
        buffer.extend_from_slice(&0i8.to_be_bytes());

        buffer
    }
}

#[allow(dead_code)]
struct RequestHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: String,
    tag_buffer: Vec<u8>,
}

fn parse_request_header(message: &[u8]) -> RequestHeader {
    let bytes = &message[0..2];
    let request_api_key = i16::from_be_bytes(bytes.try_into().unwrap());

    let bytes = &message[2..4];
    let request_api_version = i16::from_be_bytes(bytes.try_into().unwrap());

    let bytes = &message[4..8];
    let correlation_id = i32::from_be_bytes(bytes.try_into().unwrap());

    return RequestHeader {
        request_api_key,
        request_api_version,
        correlation_id,
        client_id: "".to_string(),
        tag_buffer: vec![],
    };
}

fn handle(mut stream: TcpStream) {
    loop {
        let mut message_size = [0; 4];
        if let Err(err) = stream.read_exact(&mut message_size) {
            if err.kind() == ErrorKind::UnexpectedEof {
                break;
            } else {
                panic!("Error reading message: {:?}", err);
            }
        }

        let size: usize = i32::from_be_bytes(message_size).try_into().unwrap();
        let mut message = vec![0; size];
        stream.read_exact(&mut message).unwrap();

        let header = parse_request_header(&message);
        send_api_versions_response(&mut stream, &header);
    }
}

fn send_api_versions_response(stream: &mut TcpStream, header: &RequestHeader) {
    let error_code = if header.request_api_version >= 0 && header.request_api_version <= 4 {
        ApiVersionsError::NoError
    } else {
        ApiVersionsError::UnsupportedVersion
    };

    let body = ApiVersionsV4Response {
        error_code: error_code as i16,
        api_keys: vec![
            ApiKeys {
                api_key: 18,
                min_version: 0,
                max_version: 4,
            },
            ApiKeys {
                api_key: 75,
                min_version: 0,
                max_version: 0,
            },
        ],
        throttle_time_ms: 0,
    }
    .encode();

    let message_size = (body.len() + 4) as u32;
    stream.write(&message_size.to_be_bytes()).unwrap();

    stream.write(&header.correlation_id.to_be_bytes()).unwrap();

    stream.write(&body).unwrap();
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| handle(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
