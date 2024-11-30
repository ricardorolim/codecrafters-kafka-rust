use std::io::{BufReader, Cursor, Read, Result, Write};

use crate::primitives::{
    encode_bool, encode_compact_array, encode_compact_nullable_string, encode_compact_string,
    encode_nullable_field, encode_tag_buffer, encode_varint, parse_bool,
    parse_compact_array_with_tag_buffer, parse_compact_string, parse_int16, parse_int32,
    parse_int8, parse_nullable_field, parse_tag_buffer, CompactString, Uuid,
};

pub trait Parser<T> {
    fn parse(reader: &mut impl Read) -> Result<T>;
}

pub trait Encoder {
    fn encode(&self) -> Vec<u8>;
}

#[allow(dead_code)]
pub struct ApiVersionsRequest {
    pub client_software_name: String,
    pub client_software_version: String,
}

impl Parser<Self> for ApiVersionsRequest {
    fn parse(message: &mut impl Read) -> Result<ApiVersionsRequest> {
        Ok(ApiVersionsRequest {
            client_software_name: parse_compact_string(message)?,
            client_software_version: parse_compact_string(message)?,
        })
    }
}

pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiKeys>,
    pub throttle_time_ms: i32,
}

impl ApiVersionsResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::new();

        buffer.extend(self.error_code.encode());
        buffer.extend(encode_compact_array(self.api_keys.clone()));
        buffer.extend(self.throttle_time_ms.encode());
        buffer
    }
}

#[derive(Clone)]
pub struct ApiKeys {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

impl Encoder for ApiKeys {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend(&self.api_key.to_be_bytes());
        buffer.extend(&self.min_version.to_be_bytes());
        buffer.extend(&self.max_version.to_be_bytes());
        buffer.extend(encode_tag_buffer());

        buffer
    }
}

#[allow(dead_code)]
pub struct DescribeTopicPartitionsRequest {
    pub topics: Vec<String>,
    pub response_partition_limit: i32,
    pub cursor: Option<KCursor>,
}

impl Parser<Self> for DescribeTopicPartitionsRequest {
    fn parse(reader: &mut impl Read) -> Result<Self> {
        Ok(DescribeTopicPartitionsRequest {
            topics: parse_compact_array_with_tag_buffer(reader)?
                .into_iter()
                .map(|s: CompactString| s.0)
                .collect(),
            response_partition_limit: parse_int32(reader)?,
            cursor: parse_nullable_field(reader)?,
        })
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct KCursor {
    pub topic_name: String,
    pub partition_index: i32,
}

impl Parser<Self> for KCursor {
    fn parse(reader: &mut impl Read) -> Result<Self> {
        Ok(KCursor {
            topic_name: parse_compact_string(reader)?,
            partition_index: parse_int32(reader)?,
        })
    }
}

impl Encoder for KCursor {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(encode_compact_string(self.topic_name.clone()));
        buf.extend(self.partition_index.encode());
        buf
    }
}

#[allow(dead_code)]
pub struct DescribeTopicPartitionsResponse {
    pub throttle_time_ms: i32,
    pub topics: Vec<Topic>,
    pub next_cursor: Option<KCursor>,
}

impl DescribeTopicPartitionsResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(&self.throttle_time_ms.to_be_bytes());
        buf.extend(&encode_compact_array(self.topics.clone()));
        buf.extend(encode_nullable_field(self.next_cursor.clone()));
        buf
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct Topic {
    pub error_code: ErrorCode,
    pub name: Option<String>,
    pub topic_id: Uuid,
    pub is_internal: bool,
    pub partitions: Vec<Partition>,
    pub topic_authorized_operations: i32,
}

impl Parser<Self> for Topic {
    fn parse(reader: &mut impl Read) -> Result<Self> {
        Ok(Topic {
            error_code: ErrorCode::parse(reader)?,
            name: Some(parse_compact_string(reader)?),
            topic_id: Uuid::parse(reader)?,
            is_internal: parse_bool(reader)?,
            partitions: parse_compact_array_with_tag_buffer(reader)?,
            topic_authorized_operations: parse_int32(reader)?,
        })
    }
}

impl Encoder for Topic {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(&self.error_code.encode());
        buf.extend(encode_compact_nullable_string(self.name.clone()));
        buf.extend(self.topic_id.encode());
        buf.extend(encode_bool(self.is_internal));
        buf.extend(encode_compact_array(self.partitions.clone()));
        buf.extend(self.topic_authorized_operations.encode());
        buf.extend(encode_tag_buffer());
        buf
    }
}

#[derive(Clone, PartialEq)]
#[repr(i16)]
pub enum ErrorCode {
    NoError = 0,
    UnknownTopicOrPartition = 3,
    UnsupportedVersion = 35,
}

impl Parser<Self> for ErrorCode {
    fn parse(reader: &mut impl Read) -> Result<Self> {
        let code = parse_int16(reader)?;
        let result = match code {
            value if value == ErrorCode::NoError as i16 => ErrorCode::NoError,
            value if value == ErrorCode::UnknownTopicOrPartition as i16 => {
                ErrorCode::UnknownTopicOrPartition
            }
            value if value == ErrorCode::UnsupportedVersion as i16 => ErrorCode::UnsupportedVersion,
            _ => panic!("Unknown error code: {}", code),
        };

        Ok(result)
    }
}

impl Encoder for ErrorCode {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let code = self.clone() as i16;
        buf.extend(i16::to_be_bytes(code));
        buf
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct Partition {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub eligible_leader_replicas: Vec<i32>,
    pub last_known_elr: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

impl Parser<Self> for Partition {
    fn parse(reader: &mut impl Read) -> Result<Self> {
        Ok(Partition {
            error_code: parse_int16(reader)?,
            partition_index: parse_int32(reader)?,
            leader_id: parse_int32(reader)?,
            leader_epoch: parse_int32(reader)?,
            replica_nodes: parse_compact_array_with_tag_buffer(reader)?,
            isr_nodes: parse_compact_array_with_tag_buffer(reader)?,
            eligible_leader_replicas: parse_compact_array_with_tag_buffer(reader)?,
            last_known_elr: parse_compact_array_with_tag_buffer(reader)?,
            offline_replicas: parse_compact_array_with_tag_buffer(reader)?,
        })
    }
}

impl Encoder for Partition {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.error_code.encode());
        buf.extend(self.partition_index.encode());
        buf.extend(self.leader_id.encode());
        buf.extend(self.leader_epoch.encode());
        buf.extend(encode_compact_array(self.replica_nodes.clone()));
        buf.extend(encode_compact_array(self.isr_nodes.clone()));
        buf.extend(encode_compact_array(self.eligible_leader_replicas.clone()));
        buf.extend(encode_compact_array(self.last_known_elr.clone()));
        buf.extend(encode_compact_array(self.offline_replicas.clone()));
        buf.extend(encode_tag_buffer());
        buf
    }
}
