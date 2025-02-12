use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::array_compatible;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueRef, ClickHouse};
use std::borrow::Cow;

impl Type<ClickHouse> for str {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TEXT
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        [
            ClickHouseTypeInfo::TEXT,
            ClickHouseTypeInfo::NAME,
            ClickHouseTypeInfo::BPCHAR,
            ClickHouseTypeInfo::VARCHAR,
            ClickHouseTypeInfo::UNKNOWN,
            ClickHouseTypeInfo::with_name("citext"),
        ]
        .contains(ty)
    }
}

impl Type<ClickHouse> for Cow<'_, str> {
    fn type_info() -> ClickHouseTypeInfo {
        <&str as Type<ClickHouse>>::type_info()
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        <&str as Type<ClickHouse>>::compatible(ty)
    }
}

impl Type<ClickHouse> for Box<str> {
    fn type_info() -> ClickHouseTypeInfo {
        <&str as Type<ClickHouse>>::type_info()
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        <&str as Type<ClickHouse>>::compatible(ty)
    }
}

impl Type<ClickHouse> for String {
    fn type_info() -> ClickHouseTypeInfo {
        <&str as Type<ClickHouse>>::type_info()
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        <&str as Type<ClickHouse>>::compatible(ty)
    }
}

impl ClickHouseHasArrayType for &'_ str {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TEXT_ARRAY
    }

    fn array_compatible(ty: &ClickHouseTypeInfo) -> bool {
        array_compatible::<&str>(ty)
    }
}

impl ClickHouseHasArrayType for Cow<'_, str> {
    fn array_type_info() -> ClickHouseTypeInfo {
        <&str as ClickHouseHasArrayType>::array_type_info()
    }

    fn array_compatible(ty: &ClickHouseTypeInfo) -> bool {
        <&str as ClickHouseHasArrayType>::array_compatible(ty)
    }
}

impl ClickHouseHasArrayType for Box<str> {
    fn array_type_info() -> ClickHouseTypeInfo {
        <&str as ClickHouseHasArrayType>::array_type_info()
    }

    fn array_compatible(ty: &ClickHouseTypeInfo) -> bool {
        <&str as ClickHouseHasArrayType>::array_compatible(ty)
    }
}

impl ClickHouseHasArrayType for String {
    fn array_type_info() -> ClickHouseTypeInfo {
        <&str as ClickHouseHasArrayType>::array_type_info()
    }

    fn array_compatible(ty: &ClickHouseTypeInfo) -> bool {
        <&str as ClickHouseHasArrayType>::array_compatible(ty)
    }
}

impl Encode<'_, ClickHouse> for &'_ str {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.extend(self.as_bytes());

        Ok(IsNull::No)
    }
}

impl Encode<'_, ClickHouse> for Cow<'_, str> {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        match self {
            Cow::Borrowed(str) => <&str as Encode<ClickHouse>>::encode(*str, buf),
            Cow::Owned(str) => <&str as Encode<ClickHouse>>::encode(&**str, buf),
        }
    }
}

impl Encode<'_, ClickHouse> for Box<str> {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&str as Encode<ClickHouse>>::encode(&**self, buf)
    }
}

impl Encode<'_, ClickHouse> for String {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&str as Encode<ClickHouse>>::encode(&**self, buf)
    }
}

impl<'r> Decode<'r, ClickHouse> for &'r str {
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        value.as_str()
    }
}

impl<'r> Decode<'r, ClickHouse> for Cow<'r, str> {
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(Cow::Borrowed(value.as_str()?))
    }
}

impl<'r> Decode<'r, ClickHouse> for Box<str> {
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(Box::from(value.as_str()?))
    }
}

impl Decode<'_, ClickHouse> for String {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        Ok(value.as_str()?.to_owned())
    }
}
