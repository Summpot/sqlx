use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::time::PG_EPOCH;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};
use std::mem;
use time::macros::format_description;
use time::{Date, Duration};

impl Type<ClickHouse> for Date {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::DATE
    }
}

impl ClickHouseHasArrayType for Date {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::DATE_ARRAY
    }
}

impl Encode<'_, ClickHouse> for Date {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        // DATE is encoded as number of days since epoch (2000-01-01)
        let days: i32 = (*self - PG_EPOCH).whole_days().try_into().map_err(|_| {
            format!("value {self:?} would overflow binary encoding for ClickHouse DATE")
        })?;
        Encode::<ClickHouse>::encode(days, buf)
    }

    fn size_hint(&self) -> usize {
        mem::size_of::<i32>()
    }
}

impl<'r> Decode<'r, ClickHouse> for Date {
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            ClickHouseValueFormat::Binary => {
                // DATE is encoded as the days since epoch
                let days: i32 = Decode::<ClickHouse>::decode(value)?;
                PG_EPOCH + Duration::days(days.into())
            }

            ClickHouseValueFormat::Text => Date::parse(
                value.as_str()?,
                &format_description!("[year]-[month]-[day]"),
            )?,
        })
    }
}
