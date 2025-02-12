use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};
use std::mem;
use time::macros::format_description;
use time::{Duration, Time};

impl Type<ClickHouse> for Time {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TIME
    }
}

impl ClickHouseHasArrayType for Time {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TIME_ARRAY
    }
}

impl Encode<'_, ClickHouse> for Time {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        // TIME is encoded as the microseconds since midnight.
        //
        // A truncating cast is fine because `self - Time::MIDNIGHT` cannot exceed a span of 24 hours.
        #[allow(clippy::cast_possible_truncation)]
        let micros: i64 = (*self - Time::MIDNIGHT).whole_microseconds() as i64;
        Encode::<ClickHouse>::encode(micros, buf)
    }

    fn size_hint(&self) -> usize {
        mem::size_of::<u64>()
    }
}

impl<'r> Decode<'r, ClickHouse> for Time {
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            ClickHouseValueFormat::Binary => {
                // TIME is encoded as the microseconds since midnight
                let us = Decode::<ClickHouse>::decode(value)?;
                Time::MIDNIGHT + Duration::microseconds(us)
            }

            ClickHouseValueFormat::Text => Time::parse(
                value.as_str()?,
                // ClickHouse will not include the subsecond part if it's zero.
                &format_description!("[hour]:[minute]:[second][optional [.[subsecond]]]"),
            )?,
        })
    }
}
