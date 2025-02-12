use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};

impl ClickHouseHasArrayType for u8 {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::BYTEA
    }
}

impl ClickHouseHasArrayType for &'_ [u8] {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::BYTEA_ARRAY
    }
}

impl ClickHouseHasArrayType for Box<[u8]> {
    fn array_type_info() -> ClickHouseTypeInfo {
        <[&[u8]] as Type<ClickHouse>>::type_info()
    }
}

impl ClickHouseHasArrayType for Vec<u8> {
    fn array_type_info() -> ClickHouseTypeInfo {
        <[&[u8]] as Type<ClickHouse>>::type_info()
    }
}

impl<const N: usize> ClickHouseHasArrayType for [u8; N] {
    fn array_type_info() -> ClickHouseTypeInfo {
        <[&[u8]] as Type<ClickHouse>>::type_info()
    }
}

impl Encode<'_, ClickHouse> for &'_ [u8] {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.extend_from_slice(self);

        Ok(IsNull::No)
    }
}

impl Encode<'_, ClickHouse> for Box<[u8]> {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&[u8] as Encode<ClickHouse>>::encode(self.as_ref(), buf)
    }
}

impl Encode<'_, ClickHouse> for Vec<u8> {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&[u8] as Encode<ClickHouse>>::encode(self, buf)
    }
}

impl<const N: usize> Encode<'_, ClickHouse> for [u8; N] {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&[u8] as Encode<ClickHouse>>::encode(self.as_slice(), buf)
    }
}

impl<'r> Decode<'r, ClickHouse> for &'r [u8] {
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        match value.format() {
            ClickHouseValueFormat::Binary => value.as_bytes(),
            ClickHouseValueFormat::Text => {
                Err("unsupported decode to `&[u8]` of BYTEA in a simple query; use a prepared query or decode to `Vec<u8>`".into())
            }
        }
    }
}

fn text_hex_decode_input(value: ClickHouseValueRef<'_>) -> Result<&[u8], BoxDynError> {
    // BYTEA is formatted as \x followed by hex characters
    value
        .as_bytes()?
        .strip_prefix(b"\\x")
        .ok_or("text does not start with \\x")
        .map_err(Into::into)
}

impl Decode<'_, ClickHouse> for Box<[u8]> {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            ClickHouseValueFormat::Binary => Box::from(value.as_bytes()?),
            ClickHouseValueFormat::Text => Box::from(hex::decode(text_hex_decode_input(value)?)?),
        })
    }
}

impl Decode<'_, ClickHouse> for Vec<u8> {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            ClickHouseValueFormat::Binary => value.as_bytes()?.to_owned(),
            ClickHouseValueFormat::Text => hex::decode(text_hex_decode_input(value)?)?,
        })
    }
}

impl<const N: usize> Decode<'_, ClickHouse> for [u8; N] {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        let mut bytes = [0u8; N];
        match value.format() {
            ClickHouseValueFormat::Binary => {
                bytes = value.as_bytes()?.try_into()?;
            }
            ClickHouseValueFormat::Text => hex::decode_to_slice(text_hex_decode_input(value)?, &mut bytes)?,
        };
        Ok(bytes)
    }
}
