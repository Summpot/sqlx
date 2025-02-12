use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};
use chrono::{Duration, NaiveTime};
use std::mem;

impl Type<ClickHouse> for NaiveTime {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TIME
    }
}

impl ClickHouseHasArrayType for NaiveTime {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TIME_ARRAY
    }
}

impl Encode<'_, ClickHouse> for NaiveTime {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        // TIME is encoded as the microseconds since midnight
        let micros = (*self - NaiveTime::default())
            .num_microseconds()
            .ok_or_else(|| format!("Time out of range for ClickHouse: {self}"))?;

        Encode::<ClickHouse>::encode(micros, buf)
    }

    fn size_hint(&self) -> usize {
        mem::size_of::<u64>()
    }
}

impl<'r> Decode<'r, ClickHouse> for NaiveTime {
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            ClickHouseValueFormat::Binary => {
                // TIME is encoded as the microseconds since midnight
                let us: i64 = Decode::<ClickHouse>::decode(value)?;
                NaiveTime::default() + Duration::microseconds(us)
            }

            ClickHouseValueFormat::Text => NaiveTime::parse_from_str(value.as_str()?, "%H:%M:%S%.f")?,
        })
    }
}

#[test]
fn check_naive_time_default_is_midnight() {
    // Just a canary in case this changes.
    assert_eq!(
        NaiveTime::from_hms_opt(0, 0, 0),
        Some(NaiveTime::default()),
        "implementation assumes `NaiveTime::default()` equals midnight"
    );
}
