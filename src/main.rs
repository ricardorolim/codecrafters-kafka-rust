#![allow(unused_imports)]
mod api;
mod metadata_log;
mod primitives;

use core::panic;
use std::{
    env,
    fs::File,
    io::{BufReader, Cursor, ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

use api::{
    Encoder, FetchRequest, FetchResponse, FetchResponsePartition, FetchResponseResponse, Partition,
};
use metadata_log::{ClusterMetadataLog, RecordBody, RecordType, TopicRecord};
use primitives::{encode_tag_buffer, parse_nullable_string, parse_tag_buffer, Uuid};

use crate::api::{
    ApiKeys, ApiVersionsRequest, ApiVersionsResponse, DescribeTopicPartitionsRequest,
    DescribeTopicPartitionsResponse, ErrorCode, KCursor, Parser, Topic,
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
    Fetch = 1,
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

enum RequestBody {
    Fetch(FetchRequest),
    ApiVersions(ApiVersionsRequest),
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
}

enum ResponseBody {
    Fetch(FetchResponse),
    ApiVersions(ApiVersionsResponse),
    DescribeTopicPartitions(DescribeTopicPartitionsResponse),
}

fn parse_request(message: &[u8]) -> Request {
    let mut cursor = Cursor::new(message);

    let header = parse_request_header(&mut cursor);
    let body = match header.request_api_key {
        value if value == ApiKey::Fetch as i16 => {
            let req = FetchRequest::parse(&mut cursor).expect("failed to parse Fetch request");
            RequestBody::Fetch(req)
        }
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

fn handle_request(request: &Request, metadata_log: &Arc<Mutex<ClusterMetadataLog>>) -> Response {
    let mut include_tag_buffer = true;
    let resp_body = match &request.body {
        RequestBody::Fetch(body) => {
            let resp = handle_fetch(&request.header, &body, metadata_log);
            ResponseBody::Fetch(resp)
        }
        RequestBody::ApiVersions(body) => {
            include_tag_buffer = false;
            let resp = handle_apiversions(&request.header, &body);
            ResponseBody::ApiVersions(resp)
        }
        RequestBody::DescribeTopicPartitions(body) => {
            let resp = handle_describe_topic_partitions(&request.header, &body, metadata_log);
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

fn handle_fetch(
    _header: &RequestHeader,
    request: &FetchRequest,
    metadata_log: &Arc<Mutex<ClusterMetadataLog>>,
) -> FetchResponse {
    match request.topics.first() {
        Some(topic) => {
            let message_data = metadata_log
                .lock()
                .unwrap()
                .message(&topic.topic_id)
                .expect("unable to read record batch");

            let mut error_code = ErrorCode::UnknownTopic;
            let mut records = Vec::new();

            if let Some(r) = message_data {
                records = r;
                error_code = ErrorCode::NoError;
            }

            FetchResponse {
                throttle_time_ms: 0,
                error_code: ErrorCode::NoError,
                session_id: 0,
                responses: vec![FetchResponseResponse {
                    topic_id: topic.topic_id.clone(),
                    partitions: vec![FetchResponsePartition {
                        partition_index: 0,
                        error_code,
                        high_watermark: 0,
                        last_stable_offset: 0,
                        log_start_offset: 0,
                        aborted_transactions: vec![],
                        preferred_read_replica: 0,
                        records,
                    }],
                }],
            }
        }
        None => FetchResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::NoError,
            session_id: 0,
            responses: vec![],
        },
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
                api_key: ApiKey::Fetch as i16,
                min_version: 0,
                max_version: 16,
            },
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
    request: &DescribeTopicPartitionsRequest,
    metadata_log: &Arc<Mutex<ClusterMetadataLog>>,
) -> DescribeTopicPartitionsResponse {
    let metadata = metadata_log.lock().unwrap();

    let mut topics = Vec::new();
    let mut topic_id = Uuid::new();

    for record in metadata.records() {
        if let RecordBody::Topic(topic) = &record {
            if request.topics.contains(&topic.topic_name) {
                topic_id = topic.topic_uuid.clone();

                topics.push(Topic {
                    error_code: ErrorCode::NoError,
                    name: Some(topic.topic_name.clone()),
                    topic_id: topic_id.clone(),
                    is_internal: false,
                    partitions: Vec::new(),
                    topic_authorized_operations: 0,
                });
            }
        } else if let RecordBody::Partition(partition) = record {
            if partition.topic_id == topic_id {
                let resp_partition = Partition {
                    error_code: ErrorCode::NoError,
                    partition_index: partition.partition_id,
                    leader_id: partition.leader,
                    leader_epoch: partition.leader_epoch,
                    replica_nodes: partition.replicas.clone(),
                    isr_nodes: partition.isr.clone(),
                    eligible_leader_replicas: Vec::new(),
                    last_known_elr: Vec::new(),
                    offline_replicas: Vec::new(),
                };

                topics.last_mut().unwrap().partitions.push(resp_partition);
            }
        }
    }

    if topics.len() == 0 {
        topics.push(Topic {
            error_code: ErrorCode::UnknownTopicOrPartition,
            name: Some(request.topics[0].clone()),
            topic_id: Uuid::new(),
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0,
        });
    }

    DescribeTopicPartitionsResponse {
        throttle_time_ms: 0,
        topics,
        next_cursor: None,
    }
}

fn send(stream: &mut TcpStream, response: &Response) {
    let body = match &response.body {
        ResponseBody::Fetch(r) => r.encode(),
        ResponseBody::ApiVersions(r) => r.encode(),
        ResponseBody::DescribeTopicPartitions(r) => r.encode(),
    };

    let mut msg = Vec::new();
    msg.extend(response.header.correlation_id.to_be_bytes());

    if response.header.include_tag_buffer {
        msg.extend(encode_tag_buffer());
    }

    msg.extend(body);

    stream.write_all(&(msg.len() as i32).encode()).unwrap();
    stream.write_all(&msg).unwrap();
}

fn handle_stream(mut stream: TcpStream, metadata_log: Arc<Mutex<ClusterMetadataLog>>) {
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
        let response = handle_request(&request, &metadata_log);
        send(&mut stream, &response);
    }
}

fn parse_args() -> Option<String> {
    let args: Vec<String> = env::args().collect();

    if args.len() > 0 {
        args.get(1).cloned()
    } else {
        None
    }
}

fn metadata_log() -> ClusterMetadataLog {
    let logfile = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    let props_file = parse_args();

    match props_file {
        Some(_) => ClusterMetadataLog::new(logfile),
        None => panic!("no properties file argument"),
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    let metadata_log = Arc::new(Mutex::new(metadata_log()));

    for stream in listener.incoming() {
        metadata_log
            .as_ref()
            .lock()
            .unwrap()
            .load()
            .expect("failed to read cluster metadata");

        match stream {
            Ok(stream) => {
                let log = Arc::clone(&metadata_log);
                thread::spawn(|| handle_stream(stream, log));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
