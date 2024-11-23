#![allow(unused_imports)]
mod api;
mod primitives;

use core::panic;
use std::{
    io::{Cursor, ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use api::Encoder;
use primitives::{encode_tag_buffer, parse_nullable_string, parse_tag_buffer};

use crate::api::{
    ApiKeys, ApiVersionsRequest, ApiVersionsResponse, DescribeTopicPartitionsRequest,
    DescribeTopicPartitionsResponse, ErrorCode, KCursor, Parser, Topic, Uuid,
};

struct Request {
    header: RequestHeader,
    body: RequestBody,
}

#[allow(dead_code)]
#[derive(Debug)]
struct RequestHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: String,
}

struct Response {
    header: ResponseHeader,
    body: ResponseBody,
}

struct ResponseHeader {
    correlation_id: i32,
    include_tag_buffer: bool,
}

enum ApiKey {
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

enum RequestBody {
    ApiVersions(ApiVersionsRequest),
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
}

enum ResponseBody {
    ApiVersions(ApiVersionsResponse),
    DescribeTopicPartitions(DescribeTopicPartitionsResponse),
}

fn parse_request(message: &[u8]) -> Request {
    let mut cursor = Cursor::new(message);

    let header = parse_request_header(&mut cursor);
    println!("[parse_request] header={:?}", header);
    let body = match header.request_api_key {
        value if value == ApiKey::ApiVersions as i16 => {
            let req = ApiVersionsRequest::parse(&mut cursor)
                .expect("failed to parse ApiVersions request");
            RequestBody::ApiVersions(req)
        }
        value if value == ApiKey::DescribeTopicPartitions as i16 => {
            let req = DescribeTopicPartitionsRequest::parse(&mut cursor)
                .expect("failed to parse DescribeTopicPartitions request");
            RequestBody::DescribeTopicPartitions(req)
        }
        _ => panic!("Unknown API key: {}", header.request_api_key),
    };

    Request { header, body }
}

fn parse_request_header(message: &mut impl Read) -> RequestHeader {
    let mut buf = [0; 2];
    message.read_exact(&mut buf).unwrap();
    let request_api_key = i16::from_be_bytes(buf);

    message.read_exact(&mut buf).unwrap();
    let request_api_version = i16::from_be_bytes(buf);

    let mut buf = [0; 4];
    message.read_exact(&mut buf).unwrap();
    let correlation_id = i32::from_be_bytes(buf);

    let client_id = parse_nullable_string(message).expect("failed to parse request header");
    parse_tag_buffer(message).expect("failed to parse request header");

    return RequestHeader {
        request_api_key,
        request_api_version,
        correlation_id,
        client_id,
    };
}

fn handle_request(request: &Request) -> Response {
    let mut include_tag_buffer = false;
    let resp_body = match &request.body {
        RequestBody::ApiVersions(body) => {
            let resp = handle_apiversions(&request.header, &body);
            ResponseBody::ApiVersions(resp)
        }
        RequestBody::DescribeTopicPartitions(body) => {
            include_tag_buffer = true;
            let resp = handle_describe_topic_partitions(&request.header, &body);
            ResponseBody::DescribeTopicPartitions(resp)
        }
    };

    Response {
        header: ResponseHeader {
            correlation_id: request.header.correlation_id,
            include_tag_buffer,
        },
        body: resp_body,
    }
}

fn handle_apiversions(header: &RequestHeader, _body: &ApiVersionsRequest) -> ApiVersionsResponse {
    let error_code = if header.request_api_version >= 0 && header.request_api_version <= 4 {
        ErrorCode::NoError
    } else {
        ErrorCode::UnsupportedVersion
    };

    ApiVersionsResponse {
        error_code: error_code as i16,
        api_keys: vec![
            ApiKeys {
                api_key: ApiKey::ApiVersions as i16,
                min_version: 0,
                max_version: 4,
            },
            ApiKeys {
                api_key: ApiKey::DescribeTopicPartitions as i16,
                min_version: 0,
                max_version: 0,
            },
        ],
        throttle_time_ms: 0,
    }
}

fn handle_describe_topic_partitions(
    _: &RequestHeader,
    body: &DescribeTopicPartitionsRequest,
) -> DescribeTopicPartitionsResponse {
    let topics = vec![Topic {
        error_code: ErrorCode::UnknownTopicOrPartition,
        name: Some(body.topics[0].clone()),
        topic_id: Uuid { uuid: [0; 16] },
        is_internal: false,
        partitions: Vec::new(),
        topic_authorized_operations: 0,
    }];
    DescribeTopicPartitionsResponse {
        throttle_time_ms: 0,
        topics,
        next_cursor: None,
    }
}

fn send(stream: &mut TcpStream, response: &Response) {
    let body = match &response.body {
        ResponseBody::ApiVersions(r) => r.encode(),
        ResponseBody::DescribeTopicPartitions(r) => r.encode(),
    };

    let mut msg = Vec::new();
    msg.extend(response.header.correlation_id.to_be_bytes());

    if response.header.include_tag_buffer {
        msg.extend(encode_tag_buffer());
    }

    msg.extend(body);
    msg.extend(encode_tag_buffer());

    stream.write_all(&(msg.len() as i32).encode()).unwrap();
    stream.write_all(&msg).unwrap();
}

fn handle_stream(mut stream: TcpStream) {
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

        let request = parse_request(&message);
        let response = handle_request(&request);
        send(&mut stream, &response);
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| handle_stream(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
