use mac_address::MacAddress;

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};

impl Type<ClickHouse> for MacAddress {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::MACADDR
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        *ty == ClickHouseTypeInfo::MACADDR
    }
}

impl ClickHouseHasArrayType for MacAddress {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::MACADDR_ARRAY
    }
}

impl Encode<'_, ClickHouse> for MacAddress {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.extend_from_slice(&self.bytes()); // write just the address
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        6
    }
}

impl Decode<'_, ClickHouse> for MacAddress {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        let bytes = match value.format() {
            ClickHouseValueFormat::Binary => value.as_bytes()?,
            ClickHouseValueFormat::Text => {
                return Ok(value.as_str()?.parse()?);
            }
        };

        if bytes.len() == 6 {
            return Ok(MacAddress::new(bytes.try_into().unwrap()));
        }

        Err("invalid data received when expecting an MACADDR".into())
    }
}
