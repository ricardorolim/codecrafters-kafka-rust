#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

#[allow(dead_code)]
struct RequestHeader {
    request_api_key: u16,
    request_api_version: u16,
    correlation_id: u32,
    client_id: String,
    tag_buffer: Vec<u8>,
}

fn parse_request_header(message: &[u8]) -> RequestHeader {
    let u32slice = &message[4..8];
    let correlation_id = u32::from_be_bytes(u32slice.try_into().unwrap());

    return RequestHeader {
        request_api_key: 0,
        request_api_version: 0,
        correlation_id,
        client_id: "".to_string(),
        tag_buffer: vec![],
    };
}

fn process(mut stream: TcpStream) {
    let mut message_size = [0; 4];
    stream.read_exact(&mut message_size).unwrap();

    let size: usize = u32::from_be_bytes(message_size).try_into().unwrap();
    let mut message = vec![0; size];
    stream.read_exact(&mut message).unwrap();

    let header = parse_request_header(&message);

    let message_size = 0 as u32;
    stream.write(&message_size.to_be_bytes()).unwrap();

    let correlation_id = header.correlation_id;
    stream.write(&correlation_id.to_be_bytes()).unwrap();
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                process(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
