use uuid::Uuid;

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};

impl Type<ClickHouse> for Uuid {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::UUID
    }
}

impl ClickHouseHasArrayType for Uuid {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::UUID_ARRAY
    }
}

impl Encode<'_, ClickHouse> for Uuid {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.extend_from_slice(self.as_bytes());

        Ok(IsNull::No)
    }
}

impl Decode<'_, ClickHouse> for Uuid {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        match value.format() {
            ClickHouseValueFormat::Binary => Uuid::from_slice(value.as_bytes()?),
            ClickHouseValueFormat::Text => value.as_str()?.parse(),
        }
        .map_err(Into::into)
    }
}
