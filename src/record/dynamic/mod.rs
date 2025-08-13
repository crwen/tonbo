pub(crate) mod array;
pub(crate) mod builder;
mod record;
mod record_ref;
mod schema;
mod value;

pub use array::*;
pub use arrow::datatypes::{DataType, TimeUnit};
pub use record::*;
pub use record_ref::*;
pub use schema::*;
pub use value::*;

/// Cast the `Arc<dyn Any>` to the value of given type.
#[macro_export]
macro_rules! cast_arc_value {
    ($value:expr, $type:ty) => {
        $value.as_ref().downcast_ref::<$type>().unwrap()
    };
}
