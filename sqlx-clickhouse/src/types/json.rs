use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::array_compatible;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue as JsonRawValue;
use serde_json::Value as JsonValue;
pub(crate) use sqlx_core::types::{Json, Type};

// <https://www.postgresql.org/docs/12/datatype-json.html>

// In general, most applications should prefer to store JSON data as jsonb,
// unless there are quite specialized needs, such as legacy assumptions
// about ordering of object keys.

impl<T> Type<ClickHouse> for Json<T> {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::JSONB
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        *ty == ClickHouseTypeInfo::JSON || *ty == ClickHouseTypeInfo::JSONB
    }
}

impl<T> ClickHouseHasArrayType for Json<T> {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::JSONB_ARRAY
    }

    fn array_compatible(ty: &ClickHouseTypeInfo) -> bool {
        array_compatible::<Json<T>>(ty)
    }
}

impl ClickHouseHasArrayType for JsonValue {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::JSONB_ARRAY
    }

    fn array_compatible(ty: &ClickHouseTypeInfo) -> bool {
        array_compatible::<JsonValue>(ty)
    }
}

impl ClickHouseHasArrayType for JsonRawValue {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::JSONB_ARRAY
    }

    fn array_compatible(ty: &ClickHouseTypeInfo) -> bool {
        array_compatible::<JsonRawValue>(ty)
    }
}

impl<'q, T> Encode<'q, ClickHouse> for Json<T>
where
    T: Serialize,
{
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        // we have a tiny amount of dynamic behavior depending if we are resolved to be JSON
        // instead of JSONB
        buf.patch(|buf, ty: &ClickHouseTypeInfo| {
            if *ty == ClickHouseTypeInfo::JSON || *ty == ClickHouseTypeInfo::JSON_ARRAY {
                buf[0] = b' ';
            }
        });

        // JSONB version (as of 2020-03-20)
        buf.push(1);

        // the JSON data written to the buffer is the same regardless of parameter type
        serde_json::to_writer(&mut **buf, &self.0)?;

        Ok(IsNull::No)
    }
}

impl<'r, T: 'r> Decode<'r, ClickHouse> for Json<T>
where
    T: Deserialize<'r>,
{
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        let mut buf = value.as_bytes()?;

        if value.format() == ClickHouseValueFormat::Binary && value.type_info == ClickHouseTypeInfo::JSONB {
            assert_eq!(
                buf[0], 1,
                "unsupported JSONB format version {}; please open an issue",
                buf[0]
            );

            buf = &buf[1..];
        }

        serde_json::from_slice(buf).map(Json).map_err(Into::into)
    }
}
