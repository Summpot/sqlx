use byteorder::{BigEndian, ByteOrder};

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};

impl Type<ClickHouse> for f32 {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::FLOAT4
    }
}

impl ClickHouseHasArrayType for f32 {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::FLOAT4_ARRAY
    }
}

impl Encode<'_, ClickHouse> for f32 {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.extend(&self.to_be_bytes());

        Ok(IsNull::No)
    }
}

impl Decode<'_, ClickHouse> for f32 {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            ClickHouseValueFormat::Binary => BigEndian::read_f32(value.as_bytes()?),
            ClickHouseValueFormat::Text => value.as_str()?.parse()?,
        })
    }
}

impl Type<ClickHouse> for f64 {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::FLOAT8
    }
}

impl ClickHouseHasArrayType for f64 {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::FLOAT8_ARRAY
    }
}

impl Encode<'_, ClickHouse> for f64 {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.extend(&self.to_be_bytes());

        Ok(IsNull::No)
    }
}

impl Decode<'_, ClickHouse> for f64 {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            ClickHouseValueFormat::Binary => BigEndian::read_f64(value.as_bytes()?),
            ClickHouseValueFormat::Text => value.as_str()?.parse()?,
        })
    }
}
