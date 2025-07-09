use std::{ops::Bound, sync::Arc};

use common::{Date32, Date64, Time32, Time64, Timestamp, F32, F64};
use fusio::{SeqRead, Write};
use fusio_log::{Decode, Encode};
use ulid::Ulid;

use crate::{datatype::DataType, Value};

pub type FileId = Ulid;

#[derive(Debug, Eq)]
pub(crate) struct Scope {
    pub(crate) min: Arc<dyn Value>,
    pub(crate) max: Arc<dyn Value>,
    pub(crate) gen: FileId,
    pub(crate) wal_ids: Option<Vec<FileId>>,
}

impl PartialEq for Scope {
    fn eq(&self, other: &Self) -> bool {
        self.gen.eq(&other.gen())
            && self.wal_ids.eq(&other.wal_ids)
            && self.min.eq(&other.min)
            && self.max.eq(&other.max)
    }
}

impl Clone for Scope {
    fn clone(&self) -> Self {
        Scope {
            min: self.min.clone(),
            max: self.max.clone(),
            gen: self.gen,
            wal_ids: self.wal_ids.clone(),
        }
    }
}

impl Scope {
    pub(crate) fn contains(&self, key: &dyn Value) -> bool {
        self.min.as_ref() <= key && key <= self.max.as_ref()
    }

    #[allow(unused)]
    pub(crate) fn meets(&self, target: &Self) -> bool {
        self.contains(target.min.as_ref()) || self.contains(target.max.as_ref())
    }

    pub(crate) fn meets_range(&self, range: (Bound<&dyn Value>, Bound<&dyn Value>)) -> bool {
        let excluded_contains =
            |key| -> bool { self.min.as_ref() < key && key < self.max.as_ref() };
        let included_by =
            |min, max| -> bool { min <= self.min.as_ref() && self.max.as_ref() <= max };

        match (range.0, range.1) {
            (Bound::Included(start), Bound::Included(end)) => {
                self.contains(start) || self.contains(end) || included_by(start, end)
            }
            (Bound::Included(start), Bound::Excluded(end)) => {
                start != end
                    && (self.contains(start) || excluded_contains(end) || included_by(start, end))
            }
            (Bound::Excluded(start), Bound::Included(end)) => {
                start != end
                    && (excluded_contains(start) || self.contains(end) || included_by(start, end))
            }
            (Bound::Excluded(start), Bound::Excluded(end)) => {
                start != end
                    && (excluded_contains(start)
                        || excluded_contains(end)
                        || included_by(start, end))
            }
            (Bound::Included(start), Bound::Unbounded) => start <= self.max.as_ref(),
            (Bound::Excluded(start), Bound::Unbounded) => start < self.max.as_ref(),
            (Bound::Unbounded, Bound::Included(end)) => end >= self.min.as_ref(),
            (Bound::Unbounded, Bound::Excluded(end)) => end > self.min.as_ref(),
            (Bound::Unbounded, Bound::Unbounded) => true,
        }
    }

    pub(crate) fn gen(&self) -> FileId {
        self.gen
    }
}

impl Encode for Scope {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        let data_type = self.min.data_type();
        data_type.encode(writer).await?;
        self.min.encode(writer).await?;
        self.max.encode(writer).await?;

        let (result, _) = writer.write_all(&self.gen.to_bytes()[..]).await;
        result?;

        match &self.wal_ids {
            None => {
                0u8.encode(writer).await?;
            }
            Some(ids) => {
                1u8.encode(writer).await?;
                (ids.len() as u32).encode(writer).await?;
                for id in ids {
                    let (result, _) = writer.write_all(&id.to_bytes()[..]).await;
                    result?;
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        // ProcessUniqueId: usize + u64
        self.min.size() + self.max.size() + 16
    }
}

impl Decode for Scope {
    async fn decode<R: SeqRead>(reader: &mut R) -> Result<Self, fusio::Error> {
        let mut buf = [0u8; 16];

        let data_type = DataType::decode(reader).await?;
        let (min, max): (Arc<dyn Value>, Arc<dyn Value>) = match data_type {
            DataType::UInt8 => (
                Arc::new(u8::decode(reader).await?),
                Arc::new(u8::decode(reader).await?),
            ),
            DataType::UInt16 => (
                Arc::new(u16::decode(reader).await?),
                Arc::new(u16::decode(reader).await?),
            ),
            DataType::UInt32 => (
                Arc::new(u32::decode(reader).await?),
                Arc::new(u32::decode(reader).await?),
            ),
            DataType::UInt64 => (
                Arc::new(u64::decode(reader).await?),
                Arc::new(u64::decode(reader).await?),
            ),
            DataType::Int8 => (
                Arc::new(i8::decode(reader).await?),
                Arc::new(i8::decode(reader).await?),
            ),
            DataType::Int16 => (
                Arc::new(i16::decode(reader).await?),
                Arc::new(i16::decode(reader).await?),
            ),
            DataType::Int32 => (
                Arc::new(i32::decode(reader).await?),
                Arc::new(i32::decode(reader).await?),
            ),
            DataType::Int64 => (
                Arc::new(i64::decode(reader).await?),
                Arc::new(i64::decode(reader).await?),
            ),
            DataType::Boolean => (
                Arc::new(bool::decode(reader).await?),
                Arc::new(bool::decode(reader).await?),
            ),
            DataType::String | DataType::LargeString => (
                Arc::new(String::decode(reader).await?),
                Arc::new(String::decode(reader).await?),
            ),
            DataType::Bytes | DataType::LargeBinary => (
                Arc::new(Vec::<u8>::decode(reader).await?),
                Arc::new(Vec::<u8>::decode(reader).await?),
            ),
            DataType::Float32 => (
                Arc::new(F32::decode(reader).await?),
                Arc::new(F32::decode(reader).await?),
            ),
            DataType::Float64 => (
                Arc::new(F64::decode(reader).await?),
                Arc::new(F64::decode(reader).await?),
            ),
            DataType::Timestamp(_) => (
                Arc::new(Timestamp::decode(reader).await?),
                Arc::new(Timestamp::decode(reader).await?),
            ),
            DataType::Time32(_) => (
                Arc::new(Time32::decode(reader).await?),
                Arc::new(Time32::decode(reader).await?),
            ),
            DataType::Time64(_) => (
                Arc::new(Time64::decode(reader).await?),
                Arc::new(Time64::decode(reader).await?),
            ),
            DataType::Date32 => (
                Arc::new(Date32::decode(reader).await?),
                Arc::new(Date32::decode(reader).await?),
            ),
            DataType::Date64 => (
                Arc::new(Date64::decode(reader).await?),
                Arc::new(Date64::decode(reader).await?),
            ),
        };

        let gen = {
            let (result, _) = reader.read_exact(buf.as_mut_slice()).await;
            result?;
            FileId::from_bytes(buf)
        };
        let wal_ids = match u8::decode(reader).await? {
            0 => None,
            1 => {
                let len = u32::decode(reader).await? as usize;
                let mut ids = Vec::with_capacity(len);

                for _ in 0..len {
                    let (result, _) = reader.read_exact(buf.as_mut_slice()).await;
                    result?;
                    ids.push(FileId::from_bytes(buf));
                }
                Some(ids)
            }
            _ => unreachable!(),
        };

        Ok(Scope {
            min,
            max,
            gen,
            wal_ids,
        })
    }
}

#[cfg(test)]
mod test {
    use std::{ops::Bound, sync::Arc};

    use super::Scope;

    #[tokio::test]
    async fn test_meets_range() {
        let gen = ulid::Ulid::new();
        let scope = Scope {
            min: Arc::new(100),
            max: Arc::new(200),
            gen,
            wal_ids: None,
        };

        // test out of range
        {
            assert!(!scope.meets_range((Bound::Unbounded, Bound::Excluded(&100))));
            assert!(!scope.meets_range((Bound::Unbounded, Bound::Included(&99))));
            assert!(!scope.meets_range((Bound::Unbounded, Bound::Excluded(&99))));

            assert!(!scope.meets_range((Bound::Included(&100), Bound::Excluded(&100))));
            assert!(!scope.meets_range((Bound::Excluded(&100), Bound::Included(&100))));
            assert!(!scope.meets_range((Bound::Excluded(&100), Bound::Excluded(&100))));

            assert!(!scope.meets_range((Bound::Excluded(&150), Bound::Excluded(&150))));
            assert!(!scope.meets_range((Bound::Included(&150), Bound::Excluded(&150))));
            assert!(!scope.meets_range((Bound::Excluded(&150), Bound::Included(&150))));

            assert!(!scope.meets_range((Bound::Excluded(&200), Bound::Excluded(&200))));
            assert!(!scope.meets_range((Bound::Included(&200), Bound::Excluded(&200))));
            assert!(!scope.meets_range((Bound::Excluded(&200), Bound::Included(&200))));

            assert!(!scope.meets_range((Bound::Excluded(&200), Bound::Unbounded)));
            assert!(!scope.meets_range((Bound::Included(&201), Bound::Unbounded)));
            assert!(!scope.meets_range((Bound::Excluded(&201), Bound::Unbounded)));

            assert!(!scope.meets_range((Bound::Included(&99), Bound::Excluded(&100))));
            assert!(!scope.meets_range((Bound::Excluded(&99), Bound::Excluded(&100))));
        }
        // test in range
        {
            assert!(scope.meets_range((Bound::Unbounded, Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Included(&100))));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Included(&200))));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Excluded(&200))));
            assert!(scope.meets_range((Bound::Unbounded, Bound::Included(&201))));
            assert!(scope.meets_range((Bound::Included(&200), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Included(&100), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Excluded(&100), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Included(&99), Bound::Unbounded)));
            assert!(scope.meets_range((Bound::Excluded(&99), Bound::Unbounded)));

            assert!(scope.meets_range((Bound::Included(&100), Bound::Included(&100))));
            assert!(scope.meets_range((Bound::Included(&200), Bound::Included(&200))));
            assert!(scope.meets_range((Bound::Included(&99), Bound::Included(&100))));
            assert!(scope.meets_range((Bound::Excluded(&99), Bound::Included(&100))));
            assert!(scope.meets_range((Bound::Included(&150), Bound::Included(&150))));
            assert!(scope.meets_range((Bound::Included(&100), Bound::Included(&200))));
            assert!(scope.meets_range((Bound::Included(&99), Bound::Included(&150))));
            assert!(scope.meets_range((Bound::Included(&99), Bound::Included(&201))));
            assert!(scope.meets_range((Bound::Included(&99), Bound::Excluded(&201))));
            assert!(scope.meets_range((Bound::Excluded(&99), Bound::Included(&201))));
            assert!(scope.meets_range((Bound::Excluded(&99), Bound::Excluded(&201))));
            assert!(scope.meets_range((Bound::Excluded(&100), Bound::Excluded(&200))));
        }
    }
}
