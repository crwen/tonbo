use std::{cmp::Ordering, hash::Hash, sync::Arc};

use arrow::array::{
    BinaryArray, BooleanArray, Date32Array, Date64Array, Datum, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use fusio_log::{Decode, Encode};

use super::{Key, KeyRef, TimeUnit, Value, ValueRef, F32, F64};
use crate::{datatype::DataType, key::cast::AsValue, Date32, Date64, Time32, Time64, Timestamp};

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    // TODO: Composite primary key
    pub keys: Vec<Arc<dyn Value>>,
}

impl PrimaryKey {
    pub fn new(keys: Vec<Arc<dyn Value>>) -> Self {
        Self { keys }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn arrow_datum(&self, index: usize) -> Option<Arc<dyn Datum>> {
        if self.keys.len() <= index {
            return None;
        }

        let key = &self.keys[index];
        let datum: Arc<dyn Datum> = match key.data_type() {
            DataType::UInt8 => Arc::new(UInt8Array::new_scalar(*key.as_u8())),
            DataType::UInt16 => Arc::new(UInt16Array::new_scalar(*key.as_u16())),
            DataType::UInt32 => Arc::new(UInt32Array::new_scalar(*key.as_u32())),
            DataType::UInt64 => Arc::new(UInt64Array::new_scalar(*key.as_u64())),
            DataType::Int8 => Arc::new(Int8Array::new_scalar(*key.as_i8())),
            DataType::Int16 => Arc::new(Int16Array::new_scalar(*key.as_i16())),
            DataType::Int32 => Arc::new(Int32Array::new_scalar(*key.as_i32())),
            DataType::Int64 => Arc::new(Int64Array::new_scalar(*key.as_i64())),
            DataType::Boolean => Arc::new(BooleanArray::new_scalar(*key.as_boolean())),
            DataType::String => Arc::new(StringArray::new_scalar(key.as_string())),
            DataType::LargeString => Arc::new(LargeStringArray::new_scalar(key.as_string())),
            DataType::Bytes => Arc::new(BinaryArray::new_scalar(key.as_bytes())),
            DataType::LargeBinary => Arc::new(LargeBinaryArray::new_scalar(key.as_bytes())),
            DataType::Float32 => Arc::new(Float32Array::new_scalar(key.as_f32().into())),
            DataType::Float64 => Arc::new(Float64Array::new_scalar(key.as_f64().into())),
            DataType::Timestamp(unit) => match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::new_scalar(
                    key.as_timestamp().timestamp(),
                )),
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::new_scalar(
                    key.as_timestamp().timestamp_millis(),
                )),
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::new_scalar(
                    key.as_timestamp().timestamp_micros(),
                )),
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::new_scalar(
                    key.as_timestamp().timestamp_nanos(),
                )),
            },
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => {
                    Arc::new(Time32SecondArray::new_scalar(key.as_time32().value()))
                }
                TimeUnit::Millisecond => {
                    Arc::new(Time32MillisecondArray::new_scalar(key.as_time32().value()))
                }
                TimeUnit::Microsecond => unreachable!(),
                TimeUnit::Nanosecond => unreachable!(),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => {
                    Arc::new(Time64MicrosecondArray::new_scalar(key.as_time64().value()))
                }
                TimeUnit::Nanosecond => {
                    Arc::new(Time64NanosecondArray::new_scalar(key.as_time64().value()))
                }
                TimeUnit::Millisecond => unreachable!(),
                TimeUnit::Second => unreachable!(),
            },
            DataType::Date32 => Arc::new(Date32Array::new_scalar(key.as_date32().value())),
            DataType::Date64 => Arc::new(Date64Array::new_scalar(key.as_date64().value())),
        };
        Some(datum)
    }
}

impl Key for PrimaryKey {
    type Ref<'r> = PrimaryKey;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        self.clone()
    }

    fn as_value(&self) -> &dyn Value {
        self
    }
}

impl<'r> KeyRef<'r> for PrimaryKey {
    type Key = PrimaryKey;

    fn to_key(self) -> Self::Key {
        self
    }
}

impl Value for PrimaryKey {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> crate::datatype::DataType {
        panic!("can not get data type from composite keys")
    }

    fn size_of(&self) -> usize {
        self.keys.iter().fold(4, |acc, v| acc + v.size_of() + 1)
    }

    fn is_none(&self) -> bool {
        false
    }

    fn is_some(&self) -> bool {
        false
    }

    fn clone_arc(&self) -> ValueRef {
        Arc::new(self.clone())
    }
}

impl Encode for PrimaryKey {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: fusio::Write,
    {
        let len = self.len();
        (len as u32).encode(writer).await?;
        for key in self.keys.iter() {
            key.data_type().encode(writer).await?;
            key.encode(writer).await?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.keys.iter().fold(4, |acc, v| acc + v.size() + 1)
    }
}

impl Decode for PrimaryKey {
    async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
    where
        R: fusio::SeqRead,
    {
        let len = u32::decode(reader).await?;
        let mut keys = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let data_type = DataType::decode(reader).await?;
            let key: Arc<dyn Value> = match data_type {
                DataType::UInt8 => Arc::new(u8::decode(reader).await?),
                DataType::UInt16 => Arc::new(u16::decode(reader).await?),
                DataType::UInt32 => Arc::new(u32::decode(reader).await?),
                DataType::UInt64 => Arc::new(u64::decode(reader).await?),
                DataType::Int8 => Arc::new(i8::decode(reader).await?),
                DataType::Int16 => Arc::new(i16::decode(reader).await?),
                DataType::Int32 => Arc::new(i32::decode(reader).await?),
                DataType::Int64 => Arc::new(i64::decode(reader).await?),
                DataType::String => Arc::new(String::decode(reader).await?),
                DataType::LargeString => Arc::new(String::decode(reader).await?),
                DataType::Boolean => Arc::new(bool::decode(reader).await?),
                DataType::Bytes => Arc::new(Vec::<u8>::decode(reader).await?),
                DataType::LargeBinary => Arc::new(Vec::<u8>::decode(reader).await?),
                DataType::Timestamp(_) => Arc::new(Timestamp::decode(reader).await?),
                DataType::Time32(_) => Arc::new(Time32::decode(reader).await?),
                DataType::Time64(_) => Arc::new(Time64::decode(reader).await?),
                DataType::Date32 => Arc::new(Date32::decode(reader).await?),
                DataType::Date64 => Arc::new(Date64::decode(reader).await?),
                DataType::Float32 => Arc::new(F32::decode(reader).await?),
                DataType::Float64 => Arc::new(F64::decode(reader).await?),
            };
            keys.push(key);
        }
        Ok(Self { keys })
    }
}

impl PartialOrd for PrimaryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrimaryKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (lkey, rkey) in self.keys.iter().zip(&other.keys) {
            let res = lkey.cmp(rkey);
            if res != Ordering::Equal {
                return res;
            }
        }

        Ordering::Equal
    }
}

impl Eq for PrimaryKey {}

impl PartialEq for PrimaryKey {
    fn eq(&self, other: &Self) -> bool {
        for (lkey, rkey) in self.keys.iter().zip(&other.keys) {
            if !lkey.eq(rkey) {
                return false;
            }
        }
        true
    }
}

impl Hash for PrimaryKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for key in self.keys.iter() {
            match key.data_type() {
                DataType::UInt8 => key.as_u8().hash(state),
                DataType::UInt16 => key.as_u16().hash(state),
                DataType::UInt32 => key.as_u32().hash(state),
                DataType::UInt64 => key.as_u64().hash(state),
                DataType::Int8 => key.as_i8().hash(state),
                DataType::Int16 => key.as_i16().hash(state),
                DataType::Int32 => key.as_i32().hash(state),
                DataType::Int64 => key.as_i64().hash(state),
                DataType::String => key.as_string().hash(state),
                DataType::LargeString => key.as_string().hash(state),
                DataType::Boolean => key.as_boolean().hash(state),
                DataType::Bytes => key.as_bytes().hash(state),
                DataType::LargeBinary => key.as_bytes().hash(state),
                DataType::Float32 => key.as_f32().hash(state),
                DataType::Float64 => key.as_f64().hash(state),
                DataType::Timestamp(_) => key.as_timestamp().hash(state),
                DataType::Time32(_) => key.as_time32().hash(state),
                DataType::Time64(_) => key.as_time64().hash(state),
                DataType::Date32 => key.as_date32().hash(state),
                DataType::Date64 => key.as_date64().hash(state),
            }
        }
    }
}
