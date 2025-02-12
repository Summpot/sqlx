use crate::{ClickHouseArgumentBuffer, ClickHouseTypeInfo, ClickHouseValueRef, ClickHouse};
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::{Text, Type};
use std::fmt::Display;
use std::str::FromStr;

use std::io::Write;

impl<T> Type<ClickHouse> for Text<T> {
    fn type_info() -> ClickHouseTypeInfo {
        <String as Type<ClickHouse>>::type_info()
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        <String as Type<ClickHouse>>::compatible(ty)
    }
}

impl<'q, T> Encode<'q, ClickHouse> for Text<T>
where
    T: Display,
{
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        write!(**buf, "{}", self.0)?;
        Ok(IsNull::No)
    }
}

impl<'r, T> Decode<'r, ClickHouse> for Text<T>
where
    T: FromStr,
    BoxDynError: From<<T as FromStr>::Err>,
{
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        let s: &str = Decode::<ClickHouse>::decode(value)?;
        Ok(Self(s.parse()?))
    }
}
