use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::time::PG_EPOCH;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};
use std::borrow::Cow;
use std::mem;
use time::macros::format_description;
use time::macros::offset;
use time::{Duration, OffsetDateTime, PrimitiveDateTime};

impl Type<ClickHouse> for PrimitiveDateTime {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TIMESTAMP
    }
}

impl Type<ClickHouse> for OffsetDateTime {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TIMESTAMPTZ
    }
}

impl ClickHouseHasArrayType for PrimitiveDateTime {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TIMESTAMP_ARRAY
    }
}

impl ClickHouseHasArrayType for OffsetDateTime {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TIMESTAMPTZ_ARRAY
    }
}

impl Encode<'_, ClickHouse> for PrimitiveDateTime {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        // TIMESTAMP is encoded as the microseconds since the epoch
        let micros: i64 = (*self - PG_EPOCH.midnight())
            .whole_microseconds()
            .try_into()
            .map_err(|_| {
                format!("value {self:?} would overflow binary encoding for ClickHouse TIME")
            })?;
        Encode::<ClickHouse>::encode(micros, buf)
    }

    fn size_hint(&self) -> usize {
        mem::size_of::<i64>()
    }
}

impl<'r> Decode<'r, ClickHouse> for PrimitiveDateTime {
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(match value.format() {
            ClickHouseValueFormat::Binary => {
                // TIMESTAMP is encoded as the microseconds since the epoch
                let us = Decode::<ClickHouse>::decode(value)?;
                PG_EPOCH.midnight() + Duration::microseconds(us)
            }

            ClickHouseValueFormat::Text => {
                let s = value.as_str()?;

                // If there is no decimal point we need to add one.
                let s = if s.contains('.') {
                    Cow::Borrowed(s)
                } else {
                    Cow::Owned(format!("{s}.0"))
                };

                // Contains a time-zone specifier
                // This is given for timestamptz for some reason
                // ClickHouse already guarantees this to always be UTC
                if s.contains('+') {
                    PrimitiveDateTime::parse(&s, &format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour]"))?
                } else {
                    PrimitiveDateTime::parse(
                        &s,
                        &format_description!(
                            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]"
                        ),
                    )?
                }
            }
        })
    }
}

impl Encode<'_, ClickHouse> for OffsetDateTime {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let utc = self.to_offset(offset!(UTC));
        let primitive = PrimitiveDateTime::new(utc.date(), utc.time());

        Encode::<ClickHouse>::encode(primitive, buf)
    }

    fn size_hint(&self) -> usize {
        mem::size_of::<i64>()
    }
}

impl<'r> Decode<'r, ClickHouse> for OffsetDateTime {
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(<PrimitiveDateTime as Decode<ClickHouse>>::decode(value)?.assume_utc())
    }
}
