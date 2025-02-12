use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};

impl Type<ClickHouse> for bool {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::BOOL
    }
}

impl ClickHouseHasArrayType for bool {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::BOOL_ARRAY
    }
}

impl Encode<'_, ClickHouse> for bool {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.push(*self as u8);

        Ok(IsNull::No)
    }
}

impl Decode<'_, ClickHouse> for bool {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            ClickHouseValueFormat::Binary => value.as_bytes()?[0] != 0,

            ClickHouseValueFormat::Text => match value.as_str()? {
                "t" => true,
                "f" => false,

                s => {
                    return Err(format!("unexpected value {s:?} for boolean").into());
                }
            },
        })
    }
}
