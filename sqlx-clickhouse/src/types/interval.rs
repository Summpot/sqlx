use std::mem;

use byteorder::{NetworkEndian, ReadBytesExt};

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};

// `ClickHouseInterval` is available for direct access to the INTERVAL type

#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash, Default)]
pub struct ClickHouseInterval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

impl Type<ClickHouse> for ClickHouseInterval {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INTERVAL
    }
}

impl ClickHouseHasArrayType for ClickHouseInterval {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INTERVAL_ARRAY
    }
}

impl<'de> Decode<'de, ClickHouse> for ClickHouseInterval {
    fn decode(value: ClickHouseValueRef<'de>) -> Result<Self, BoxDynError> {
        match value.format() {
            ClickHouseValueFormat::Binary => {
                let mut buf = value.as_bytes()?;
                let microseconds = buf.read_i64::<NetworkEndian>()?;
                let days = buf.read_i32::<NetworkEndian>()?;
                let months = buf.read_i32::<NetworkEndian>()?;

                Ok(ClickHouseInterval {
                    months,
                    days,
                    microseconds,
                })
            }

            // TODO: Implement parsing of text mode
            ClickHouseValueFormat::Text => {
                Err("not implemented: decode `INTERVAL` in text mode (unprepared queries)".into())
            }
        }
    }
}

impl Encode<'_, ClickHouse> for ClickHouseInterval {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.extend(&self.microseconds.to_be_bytes());
        buf.extend(&self.days.to_be_bytes());
        buf.extend(&self.months.to_be_bytes());

        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        2 * mem::size_of::<i64>()
    }
}

// We then implement Encode + Type for std Duration, chrono Duration, and time Duration
// This is to enable ease-of-use for encoding when its simple

impl Type<ClickHouse> for std::time::Duration {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INTERVAL
    }
}

impl ClickHouseHasArrayType for std::time::Duration {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INTERVAL_ARRAY
    }
}

impl Encode<'_, ClickHouse> for std::time::Duration {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        ClickHouseInterval::try_from(*self)?.encode_by_ref(buf)
    }

    fn size_hint(&self) -> usize {
        2 * mem::size_of::<i64>()
    }
}

impl TryFrom<std::time::Duration> for ClickHouseInterval {
    type Error = BoxDynError;

    /// Convert a `std::time::Duration` to a `ClickHouseInterval`
    ///
    /// This returns an error if there is a loss of precision using nanoseconds or if there is a
    /// microsecond overflow.
    fn try_from(value: std::time::Duration) -> Result<Self, BoxDynError> {
        if value.as_nanos() % 1000 != 0 {
            return Err("ClickHouse `INTERVAL` does not support nanoseconds precision".into());
        }

        Ok(Self {
            months: 0,
            days: 0,
            microseconds: value.as_micros().try_into()?,
        })
    }
}

#[cfg(feature = "chrono")]
impl Type<ClickHouse> for chrono::Duration {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INTERVAL
    }
}

#[cfg(feature = "chrono")]
impl ClickHouseHasArrayType for chrono::Duration {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INTERVAL_ARRAY
    }
}

#[cfg(feature = "chrono")]
impl Encode<'_, ClickHouse> for chrono::Duration {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let pg_interval = ClickHouseInterval::try_from(*self)?;
        pg_interval.encode_by_ref(buf)
    }

    fn size_hint(&self) -> usize {
        2 * mem::size_of::<i64>()
    }
}

#[cfg(feature = "chrono")]
impl TryFrom<chrono::Duration> for ClickHouseInterval {
    type Error = BoxDynError;

    /// Convert a `chrono::Duration` to a `ClickHouseInterval`.
    ///
    /// This returns an error if there is a loss of precision using nanoseconds or if there is a
    /// nanosecond overflow.
    fn try_from(value: chrono::Duration) -> Result<Self, BoxDynError> {
        value
            .num_nanoseconds()
            .map_or::<Result<_, Self::Error>, _>(
                Err("Overflow has occurred for ClickHouse `INTERVAL`".into()),
                |nanoseconds| {
                    if nanoseconds % 1000 != 0 {
                        return Err(
                            "ClickHouse `INTERVAL` does not support nanoseconds precision".into(),
                        );
                    }
                    Ok(())
                },
            )?;

        value.num_microseconds().map_or(
            Err("Overflow has occurred for ClickHouse `INTERVAL`".into()),
            |microseconds| {
                Ok(Self {
                    months: 0,
                    days: 0,
                    microseconds,
                })
            },
        )
    }
}

#[cfg(feature = "time")]
impl Type<ClickHouse> for time::Duration {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INTERVAL
    }
}

#[cfg(feature = "time")]
impl ClickHouseHasArrayType for time::Duration {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INTERVAL_ARRAY
    }
}

#[cfg(feature = "time")]
impl Encode<'_, ClickHouse> for time::Duration {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let pg_interval = ClickHouseInterval::try_from(*self)?;
        pg_interval.encode_by_ref(buf)
    }

    fn size_hint(&self) -> usize {
        2 * mem::size_of::<i64>()
    }
}

#[cfg(feature = "time")]
impl TryFrom<time::Duration> for ClickHouseInterval {
    type Error = BoxDynError;

    /// Convert a `time::Duration` to a `ClickHouseInterval`.
    ///
    /// This returns an error if there is a loss of precision using nanoseconds or if there is a
    /// microsecond overflow.
    fn try_from(value: time::Duration) -> Result<Self, BoxDynError> {
        if value.whole_nanoseconds() % 1000 != 0 {
            return Err("ClickHouse `INTERVAL` does not support nanoseconds precision".into());
        }

        Ok(Self {
            months: 0,
            days: 0,
            microseconds: value.whole_microseconds().try_into()?,
        })
    }
}

#[test]
fn test_encode_interval() {
    let mut buf = ClickHouseArgumentBuffer::default();

    let interval = ClickHouseInterval {
        months: 0,
        days: 0,
        microseconds: 0,
    };
    assert!(matches!(
        Encode::<ClickHouse>::encode(&interval, &mut buf),
        Ok(IsNull::No)
    ));
    assert_eq!(&**buf, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    buf.clear();

    let interval = ClickHouseInterval {
        months: 0,
        days: 0,
        microseconds: 1_000,
    };
    assert!(matches!(
        Encode::<ClickHouse>::encode(&interval, &mut buf),
        Ok(IsNull::No)
    ));
    assert_eq!(&**buf, [0, 0, 0, 0, 0, 0, 3, 232, 0, 0, 0, 0, 0, 0, 0, 0]);
    buf.clear();

    let interval = ClickHouseInterval {
        months: 0,
        days: 0,
        microseconds: 1_000_000,
    };
    assert!(matches!(
        Encode::<ClickHouse>::encode(&interval, &mut buf),
        Ok(IsNull::No)
    ));
    assert_eq!(&**buf, [0, 0, 0, 0, 0, 15, 66, 64, 0, 0, 0, 0, 0, 0, 0, 0]);
    buf.clear();

    let interval = ClickHouseInterval {
        months: 0,
        days: 0,
        microseconds: 3_600_000_000,
    };
    assert!(matches!(
        Encode::<ClickHouse>::encode(&interval, &mut buf),
        Ok(IsNull::No)
    ));
    assert_eq!(
        &**buf,
        [0, 0, 0, 0, 214, 147, 164, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    buf.clear();

    let interval = ClickHouseInterval {
        months: 0,
        days: 1,
        microseconds: 0,
    };
    assert!(matches!(
        Encode::<ClickHouse>::encode(&interval, &mut buf),
        Ok(IsNull::No)
    ));
    assert_eq!(&**buf, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]);
    buf.clear();

    let interval = ClickHouseInterval {
        months: 1,
        days: 0,
        microseconds: 0,
    };
    assert!(matches!(
        Encode::<ClickHouse>::encode(&interval, &mut buf),
        Ok(IsNull::No)
    ));
    assert_eq!(&**buf, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    buf.clear();

    assert_eq!(
        ClickHouseInterval::default(),
        ClickHouseInterval {
            months: 0,
            days: 0,
            microseconds: 0,
        }
    );
}

#[test]
fn test_pginterval_std() {
    // Case for positive duration
    let interval = ClickHouseInterval {
        days: 0,
        months: 0,
        microseconds: 27_000,
    };
    assert_eq!(
        &ClickHouseInterval::try_from(std::time::Duration::from_micros(27_000)).unwrap(),
        &interval
    );

    // Case when precision loss occurs
    assert!(ClickHouseInterval::try_from(std::time::Duration::from_nanos(27_000_001)).is_err());

    // Case when microsecond overflow occurs
    assert!(ClickHouseInterval::try_from(std::time::Duration::from_secs(20_000_000_000_000)).is_err());
}

#[test]
#[cfg(feature = "chrono")]
fn test_pginterval_chrono() {
    // Case for positive duration
    let interval = ClickHouseInterval {
        days: 0,
        months: 0,
        microseconds: 27_000,
    };
    assert_eq!(
        &ClickHouseInterval::try_from(chrono::Duration::microseconds(27_000)).unwrap(),
        &interval
    );

    // Case for negative duration
    let interval = ClickHouseInterval {
        days: 0,
        months: 0,
        microseconds: -27_000,
    };
    assert_eq!(
        &ClickHouseInterval::try_from(chrono::Duration::microseconds(-27_000)).unwrap(),
        &interval
    );

    // Case when precision loss occurs
    assert!(ClickHouseInterval::try_from(chrono::Duration::nanoseconds(27_000_001)).is_err());
    assert!(ClickHouseInterval::try_from(chrono::Duration::nanoseconds(-27_000_001)).is_err());

    // Case when nanosecond overflow occurs
    assert!(ClickHouseInterval::try_from(chrono::Duration::seconds(10_000_000_000)).is_err());
    assert!(ClickHouseInterval::try_from(chrono::Duration::seconds(-10_000_000_000)).is_err());
}

#[test]
#[cfg(feature = "time")]
fn test_pginterval_time() {
    // Case for positive duration
    let interval = ClickHouseInterval {
        days: 0,
        months: 0,
        microseconds: 27_000,
    };
    assert_eq!(
        &ClickHouseInterval::try_from(time::Duration::microseconds(27_000)).unwrap(),
        &interval
    );

    // Case for negative duration
    let interval = ClickHouseInterval {
        days: 0,
        months: 0,
        microseconds: -27_000,
    };
    assert_eq!(
        &ClickHouseInterval::try_from(time::Duration::microseconds(-27_000)).unwrap(),
        &interval
    );

    // Case when precision loss occurs
    assert!(ClickHouseInterval::try_from(time::Duration::nanoseconds(27_000_001)).is_err());
    assert!(ClickHouseInterval::try_from(time::Duration::nanoseconds(-27_000_001)).is_err());

    // Case when microsecond overflow occurs
    assert!(ClickHouseInterval::try_from(time::Duration::seconds(10_000_000_000_000)).is_err());
    assert!(ClickHouseInterval::try_from(time::Duration::seconds(-10_000_000_000_000)).is_err());
}
