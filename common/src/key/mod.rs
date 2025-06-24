mod bool;
mod cast;
mod datetime;
mod list;
mod num;
mod str;
mod timestamp;

use std::{any::Any, fmt::Debug, hash::Hash, sync::Arc};

use arrow::array::{
    AsArray, Date32Array, Date64Array, Datum, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
pub use datetime::*;
use fusio_log::{Decode, Encode};
pub use list::*;
pub use num::*;
pub use str::*;
pub use timestamp::*;

// - 如何替换 is_nullable?
// - 如何表示 Option<T>
use crate::{datatype::DataType, key::cast::AsValue};
pub trait Value: 'static + Send + Sync + Debug {
    fn as_any(&self) -> &dyn Any;

    fn data_type(&self) -> DataType;

    fn size_of(&self) -> usize;

    fn to_arrow_datum(&self) -> Arc<dyn Datum>;

    fn is_none(&self) -> bool;

    fn is_some(&self) -> bool;
}

pub trait Key:
    'static + Value + Encode + Decode + Ord + Clone + Send + Sync + Hash + std::fmt::Debug
{
    type Ref<'r>: KeyRef<'r, Key = Self>
    where
        Self: 'r;

    fn as_key_ref(&self) -> Self::Ref<'_>;

    fn to_arrow_datum(&self) -> Arc<dyn Datum>;

    fn as_value(&self) -> &dyn Value;
}

pub trait KeyRef<'r>: Clone + Encode + Send + Sync + Ord + std::fmt::Debug {
    type Key: Key<Ref<'r> = Self>;

    fn to_key(self) -> Self::Key;
}

impl Encode for dyn Value {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: fusio::Write,
    {
        let data_type = self.data_type();
        // downcast to concrete type
        match &data_type {
            DataType::UInt8 => {
                self.as_any()
                    .downcast_ref::<u8>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::UInt16 => {
                self.as_any()
                    .downcast_ref::<u16>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::UInt32 => {
                self.as_any()
                    .downcast_ref::<u32>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::UInt64 => {
                self.as_any()
                    .downcast_ref::<u64>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Int8 => {
                self.as_any()
                    .downcast_ref::<i8>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Int16 => {
                self.as_any()
                    .downcast_ref::<i16>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Int32 => {
                self.as_any()
                    .downcast_ref::<i32>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Int64 => {
                self.as_any()
                    .downcast_ref::<i64>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::String | DataType::LargeString => {
                self.as_any()
                    .downcast_ref::<String>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Boolean => {
                self.as_any()
                    .downcast_ref::<bool>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Bytes | DataType::LargeBinary => {
                self.as_any()
                    .downcast_ref::<Vec<u8>>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Float32 => {
                self.as_any()
                    .downcast_ref::<F32>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Float64 => {
                self.as_any()
                    .downcast_ref::<F64>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Timestamp(_) => {
                self.as_any()
                    .downcast_ref::<Timestamp>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Time32(_) => {
                self.as_any()
                    .downcast_ref::<Time32>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Time64(_) => {
                self.as_any()
                    .downcast_ref::<Time64>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Date32 => {
                self.as_any()
                    .downcast_ref::<Date32>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
            DataType::Date64 => {
                self.as_any()
                    .downcast_ref::<Date64>()
                    .unwrap()
                    .encode(writer)
                    .await?
            }
        };
        Ok(())
    }

    fn size(&self) -> usize {
        self.size_of()
    }
}

impl Eq for dyn Value {}
impl PartialEq for dyn Value {
    fn eq(&self, other: &Self) -> bool {
        if self.data_type() != other.data_type() {
            return false;
        }
        if self.is_none() || self.is_some() {
            match self.data_type() {
                DataType::UInt8 => self
                    .as_any()
                    .downcast_ref::<Option<u8>>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<Option<u8>>().unwrap()),
                DataType::UInt16 => self
                    .as_any()
                    .downcast_ref::<Option<u16>>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<Option<u16>>().unwrap()),
                DataType::UInt32 => self
                    .as_any()
                    .downcast_ref::<Option<u32>>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<Option<u32>>().unwrap()),
                DataType::UInt64 => self
                    .as_any()
                    .downcast_ref::<Option<u64>>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<Option<u64>>().unwrap()),
                DataType::Int8 => todo!(),
                DataType::Int16 => todo!(),
                DataType::Int32 => todo!(),
                DataType::Int64 => todo!(),
                DataType::String => todo!(),
                DataType::LargeString => todo!(),
                DataType::Boolean => todo!(),
                DataType::Bytes => todo!(),
                DataType::LargeBinary => todo!(),
                DataType::Float32 => todo!(),
                DataType::Float64 => todo!(),
                DataType::Timestamp(time_unit) => todo!(),
                DataType::Time32(time_unit) => todo!(),
                DataType::Time64(time_unit) => todo!(),
                DataType::Date32 => todo!(),
                DataType::Date64 => todo!(),
            }
        } else {
            match self.data_type() {
                DataType::UInt8 => self
                    .as_any()
                    .downcast_ref::<u8>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<u8>().unwrap()),
                DataType::UInt16 => self
                    .as_any()
                    .downcast_ref::<u16>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<u16>().unwrap()),
                DataType::UInt32 => self
                    .as_any()
                    .downcast_ref::<u32>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<u32>().unwrap()),
                DataType::UInt64 => self
                    .as_any()
                    .downcast_ref::<u64>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<u64>().unwrap()),
                DataType::Int8 => self
                    .as_any()
                    .downcast_ref::<i8>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<i8>().unwrap()),
                DataType::Int16 => self
                    .as_any()
                    .downcast_ref::<i16>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<i16>().unwrap()),
                DataType::Int32 => self
                    .as_any()
                    .downcast_ref::<i32>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<i32>().unwrap()),
                DataType::Int64 => self
                    .as_any()
                    .downcast_ref::<i64>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<i64>().unwrap()),
                DataType::Boolean => self
                    .as_any()
                    .downcast_ref::<bool>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<bool>().unwrap()),
                DataType::String | DataType::LargeString => self
                    .as_any()
                    .downcast_ref::<String>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<String>().unwrap()),
                DataType::Bytes | DataType::LargeBinary => self
                    .as_any()
                    .downcast_ref::<Vec<u8>>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<Vec<u8>>().unwrap()),
                DataType::Float32 => self
                    .as_any()
                    .downcast_ref::<F32>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<F32>().unwrap()),
                DataType::Float64 => self
                    .as_any()
                    .downcast_ref::<F64>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<F64>().unwrap()),
                DataType::Timestamp(_) => self
                    .as_any()
                    .downcast_ref::<Timestamp>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<Timestamp>().unwrap()),
                DataType::Time32(_) => self
                    .as_any()
                    .downcast_ref::<Time32>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<Time32>().unwrap()),
                DataType::Time64(_) => self
                    .as_any()
                    .downcast_ref::<Time64>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<Time64>().unwrap()),
                DataType::Date32 => self
                    .as_any()
                    .downcast_ref::<Date32>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<Date32>().unwrap()),
                DataType::Date64 => self
                    .as_any()
                    .downcast_ref::<Date64>()
                    .unwrap()
                    .eq(other.as_any().downcast_ref::<Date64>().unwrap()),
            }
        }
    }
}

impl PartialOrd for dyn Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.data_type() {
            DataType::UInt8 => self
                .as_any()
                .downcast_ref::<u8>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<u8>().unwrap()),
            DataType::UInt16 => self
                .as_any()
                .downcast_ref::<u16>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<u16>().unwrap()),
            DataType::UInt32 => self
                .as_any()
                .downcast_ref::<u32>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<u32>().unwrap()),
            DataType::UInt64 => self
                .as_any()
                .downcast_ref::<u64>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<u64>().unwrap()),
            DataType::Int8 => self
                .as_any()
                .downcast_ref::<i8>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<i8>().unwrap()),
            DataType::Int16 => self
                .as_any()
                .downcast_ref::<i16>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<i16>().unwrap()),
            DataType::Int32 => self
                .as_any()
                .downcast_ref::<i32>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<i32>().unwrap()),
            DataType::Int64 => self
                .as_any()
                .downcast_ref::<i64>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<i64>().unwrap()),
            DataType::Boolean => self
                .as_any()
                .downcast_ref::<bool>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<bool>().unwrap()),
            DataType::String | DataType::LargeString => self
                .as_any()
                .downcast_ref::<String>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<String>().unwrap()),
            DataType::Bytes | DataType::LargeBinary => self
                .as_any()
                .downcast_ref::<Vec<u8>>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<Vec<u8>>().unwrap()),
            DataType::Float32 => self
                .as_any()
                .downcast_ref::<F32>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<F32>().unwrap()),
            DataType::Float64 => self
                .as_any()
                .downcast_ref::<F64>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<F64>().unwrap()),
            DataType::Timestamp(_) => self
                .as_any()
                .downcast_ref::<Timestamp>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<Timestamp>().unwrap()),
            DataType::Time32(_) => self
                .as_any()
                .downcast_ref::<Time32>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<Time32>().unwrap()),
            DataType::Time64(_) => self
                .as_any()
                .downcast_ref::<Time64>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<Time64>().unwrap()),
            DataType::Date32 => self
                .as_any()
                .downcast_ref::<Date32>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<Date32>().unwrap()),
            DataType::Date64 => self
                .as_any()
                .downcast_ref::<Date64>()
                .unwrap()
                .cmp(other.as_any().downcast_ref::<Date64>().unwrap()),
        }
    }
}

#[cfg(test)]
mod tests {
    //     // use crossbeam_skiplist::SkipMap;
    //     // use fusio_log::{Decode, Encode};
    //     // use std::io::{Cursor, SeekFrom};
    //     // use tokio::io::AsyncSeekExt;

    //     // use crate::record::{Key, F32};

    //     // use super::Value;

    use crate::{Date32, Key};

    #[test]
    fn test_value_eq() {
        {
            let val = 123i32.as_value();
            let val2 = 123i32.as_value();
            let val3 = 123i8.as_value();
            let val4 = 124i32.as_value();
            let val5 = Date32::from(123);
            assert_eq!(val, val2);
            assert_ne!(val, val3);
            assert_ne!(val, val4);
            assert_ne!(val, val5.as_value());
        }
        // {
        //     let val = String::from("123").as_value();
        //     let val2 = String::from("123").as_value();
        //     let val3 = Da;
        //     let val = String::from("124").as_value();
        //     assert_eq!(val, val2);
        //     assert_ne!(val, val3);
        //     assert_ne!(val, val4);
        // }
    }

    #[test]
    fn test_value_cmp() {
        {
            let val = 123u32.as_value();
            let val2 = 123u32.as_value();
            let val3 = 124u32.as_value();
            let val4 = 122u32.as_value();
            assert_eq!(val.cmp(val2), std::cmp::Ordering::Equal);
            assert_eq!(val.cmp(val3), std::cmp::Ordering::Less);
            assert_eq!(val.cmp(val4), std::cmp::Ordering::Greater);
        }
    }

    //     // #[tokio::test]
    //     // async fn test_encode_value_trait() {
    //     // let value = 123u16;
    //     // let mut bytes = Vec::new();
    //     // let mut buf = Cursor::new(&mut bytes);
    //     // value.encode(&mut buf).await.unwrap();
    //     // // let record_ref = record.as_record_ref();
    //     // // record_ref.encode(&mut buf).await.unwrap();

    //     // buf.seek(SeekFrom::Start(0)).await.unwrap();
    //     // let decoded = u16::decode(&mut buf).await.unwrap();

    //     // assert_eq!(value, decoded);

    //     // let map = SkipMap::<u16, Box<dyn Value>>::new();
    //     // map.insert(123u16, Box::new(1u8));
    //     // map.insert(12u16, Box::new(12345u16));
    //     // map.insert(13u16, Box::new(F32::from(12345.123f32)));
    //     // map.insert(23u16, Box::new(12u8));
    //     // map.insert(3u16, Box::new("String".to_string()));

    //     // for entry in map.iter() {
    //     //     let key = entry.key();
    //     //     let val = entry.value();
    //     //     let dyn_key = key.as_value();
    //     //     println!("key: {:?}, value: {:?}, dyn_key: {:?}", key, val, dyn_key);
    //     //     // dbg!(key, val, dyn_key);
    //     // }
    //     // println!("{:#?}", map);
    //     // dbg!(map.values());

    //     // map.insert(123u16, "a".into());
    //     // }
}
