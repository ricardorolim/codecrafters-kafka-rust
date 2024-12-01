use core::panic;
use std::{
    collections::binary_heap::Iter,
    fs::File,
    io::{BufRead, BufReader, Read, Result},
};

use bytes::buf::Reader;

use crate::{
    api::{Parser, Partition, Topic},
    primitives::{
        parse_compact_array, parse_compact_string, parse_int16, parse_int32, parse_int64,
        parse_int8, parse_nullable_string, parse_unsigned_varint, parse_unsigned_varlong,
        parse_varint, Uuid,
    },
};

#[derive(Debug)]
#[allow(dead_code)]
pub struct ClusterMetadataLog {
    logfile: String,
    loaded: bool,
    pub batches: Vec<Batch>,
}

impl ClusterMetadataLog {
    pub fn new(logfile: &str) -> ClusterMetadataLog {
        ClusterMetadataLog {
            logfile: logfile.to_string(),
            batches: Vec::new(),
            loaded: false,
        }
    }

    pub fn load(&mut self) -> Result<()> {
        if self.loaded {
            return Ok(());
        }

        let file =
            File::open(self.logfile.clone()).expect("failed to open cluster metadata log file");
        let mut reader = BufReader::new(file);

        let mut batches = Vec::new();

        while !reader.fill_buf()?.is_empty() {
            batches.push(Batch::parse(&mut reader)?);
        }

        self.batches = batches;
        self.loaded = true;

        Ok(())
    }

    pub fn records(&self) -> Vec<RecordBody> {
        self.batches
            .iter()
            .flat_map(|batch| batch.records.iter())
            .map(|record| record.value.body.clone())
            .collect()
    }

    pub fn topics(&self) -> Vec<TopicRecord> {
        self.records()
            .iter()
            .filter_map(|record| {
                if let RecordBody::Topic(topic) = record {
                    Some(topic.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Batch {
    base_offset: i64,
    base_length: i32,
    partition_leader_epoch: i32,
    magic_byte: i8,
    crc: u32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    pub records: Vec<Record>,
}

impl Batch {
    pub fn parse(reader: &mut impl Read) -> Result<Batch> {
        Ok(Batch {
            base_offset: parse_int64(reader)?,
            base_length: parse_int32(reader)?,
            partition_leader_epoch: parse_int32(reader)?,
            magic_byte: parse_int8(reader)?,
            crc: parse_int32(reader)? as u32,
            attributes: parse_int16(reader)?,
            last_offset_delta: parse_int32(reader)?,
            base_timestamp: parse_int64(reader)?,
            max_timestamp: parse_int64(reader)?,
            producer_id: parse_int64(reader)?,
            producer_epoch: parse_int16(reader)?,
            base_sequence: parse_int32(reader)?,
            records: (0..parse_int32(reader)?)
                .map(|_| Record::parse(reader).unwrap())
                .collect(),
        })
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Record {
    length: i32,
    attributes: i8,
    timestamp_delta: i64,
    offset_delta: i32,
    key: Option<String>,
    value_length: i32,
    pub value: RecordValue,
    headers_array_count: u32,
}

impl Record {
    pub fn parse(reader: &mut impl Read) -> Result<Record> {
        Ok(Record {
            length: parse_varint(reader)?,
            attributes: parse_int8(reader)?,
            timestamp_delta: parse_unsigned_varlong(reader)? as i64,
            offset_delta: parse_varint(reader)?,
            key: Some(parse_compact_string(reader)?),
            value_length: parse_varint(reader)?,
            value: RecordValue::parse(reader)?,
            headers_array_count: parse_unsigned_varint(reader)?,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct RecordValue {
    header: RecordHeader,
    pub body: RecordBody,
    tagged_fields_count: u32,
}

impl RecordValue {
    fn parse(reader: &mut impl Read) -> Result<RecordValue> {
        let header = RecordHeader::parse(reader)?;

        let body = match header.rtype {
            RecordType::Topic => RecordBody::Topic(TopicRecord::parse(reader)?),
            RecordType::Partition => RecordBody::Partition(PartitionRecord::parse(reader)?),
            RecordType::FeatureLevel => {
                RecordBody::FeatureLevel(FeatureLevelRecord::parse(reader)?)
            }
        };

        Ok(RecordValue {
            header,
            body,
            tagged_fields_count: parse_unsigned_varint(reader)?,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct RecordHeader {
    frame_version: i8,
    rtype: RecordType,
    version: i8,
}

impl RecordHeader {
    fn parse(reader: &mut impl Read) -> Result<Self> {
        Ok(RecordHeader {
            frame_version: parse_int8(reader)?,
            rtype: RecordType::parse(reader)?,
            version: parse_int8(reader)?,
        })
    }
}

#[derive(Debug)]
#[repr(i8)]
pub enum RecordType {
    Topic = 2,
    Partition = 3,
    FeatureLevel = 12,
}

impl RecordType {
    fn parse(reader: &mut impl Read) -> Result<Self> {
        let rtype = parse_int8(reader)?;

        let r = match rtype {
            value if value == RecordType::Topic as i8 => RecordType::Topic,
            value if value == RecordType::Partition as i8 => RecordType::Partition,
            value if value == RecordType::FeatureLevel as i8 => RecordType::FeatureLevel,
            _ => panic!(),
        };

        Ok(r)
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum RecordBody {
    Topic(TopicRecord),
    Partition(PartitionRecord),
    FeatureLevel(FeatureLevelRecord),
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct TopicRecord {
    pub topic_name: String,
    pub topic_uuid: Uuid,
}

impl TopicRecord {
    fn parse(reader: &mut impl Read) -> Result<TopicRecord> {
        Ok(TopicRecord {
            topic_name: parse_compact_string(reader)?,
            topic_uuid: Uuid::parse(reader)?,
        })
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct PartitionRecord {
    pub partition_id: i32,
    pub topic_id: Uuid,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub removing_replicas: Vec<i32>,
    pub adding_replicas: Vec<i32>,
    pub leader: i32,
    pub leader_epoch: i32,
    pub partition_epoch: i32,
    pub directories: Vec<Uuid>,
}

#[allow(dead_code)]
impl PartitionRecord {
    fn parse(reader: &mut impl Read) -> Result<Self> {
        Ok(PartitionRecord {
            partition_id: parse_int32(reader)?,
            topic_id: Uuid::parse(reader)?,
            replicas: parse_compact_array(reader)?,
            isr: parse_compact_array(reader)?,
            removing_replicas: parse_compact_array(reader)?,
            adding_replicas: parse_compact_array(reader)?,
            leader: parse_int32(reader)?,
            leader_epoch: parse_int32(reader)?,
            partition_epoch: parse_int32(reader)?,
            directories: parse_compact_array(reader)?,
        })
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct FeatureLevelRecord {
    name: String,
    feature_level: i16,
}

impl FeatureLevelRecord {
    fn parse(reader: &mut impl Read) -> Result<FeatureLevelRecord> {
        Ok(FeatureLevelRecord {
            name: parse_compact_string(reader)?,
            feature_level: parse_int16(reader)?,
        })
    }
}
