use std::fmt::{self, Debug, Display, Formatter};
use std::ops::{Bound, Range, RangeBounds, RangeFrom, RangeInclusive, RangeTo, RangeToInclusive};

use bitflags::bitflags;
use sqlx_core::bytes::Buf;

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::type_info::ClickHouseTypeKind;
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};

// https://github.com/postgres/postgres/blob/2f48ede080f42b97b594fb14102c82ca1001b80c/src/include/utils/rangetypes.h#L35-L44
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct RangeFlags: u8 {
        const EMPTY = 0x01;
        const LB_INC = 0x02;
        const UB_INC = 0x04;
        const LB_INF = 0x08;
        const UB_INF = 0x10;
        const LB_NULL = 0x20; // not used
        const UB_NULL = 0x40; // not used
        const CONTAIN_EMPTY = 0x80; // internal
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ClickHouseRange<T> {
    pub start: Bound<T>,
    pub end: Bound<T>,
}

impl<T> From<[Bound<T>; 2]> for ClickHouseRange<T> {
    fn from(v: [Bound<T>; 2]) -> Self {
        let [start, end] = v;
        Self { start, end }
    }
}

impl<T> From<(Bound<T>, Bound<T>)> for ClickHouseRange<T> {
    fn from(v: (Bound<T>, Bound<T>)) -> Self {
        Self {
            start: v.0,
            end: v.1,
        }
    }
}

impl<T> From<Range<T>> for ClickHouseRange<T> {
    fn from(v: Range<T>) -> Self {
        Self {
            start: Bound::Included(v.start),
            end: Bound::Excluded(v.end),
        }
    }
}

impl<T> From<RangeFrom<T>> for ClickHouseRange<T> {
    fn from(v: RangeFrom<T>) -> Self {
        Self {
            start: Bound::Included(v.start),
            end: Bound::Unbounded,
        }
    }
}

impl<T> From<RangeInclusive<T>> for ClickHouseRange<T> {
    fn from(v: RangeInclusive<T>) -> Self {
        let (start, end) = v.into_inner();
        Self {
            start: Bound::Included(start),
            end: Bound::Included(end),
        }
    }
}

impl<T> From<RangeTo<T>> for ClickHouseRange<T> {
    fn from(v: RangeTo<T>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Excluded(v.end),
        }
    }
}

impl<T> From<RangeToInclusive<T>> for ClickHouseRange<T> {
    fn from(v: RangeToInclusive<T>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Included(v.end),
        }
    }
}

impl<T> RangeBounds<T> for ClickHouseRange<T> {
    fn start_bound(&self) -> Bound<&T> {
        match self.start {
            Bound::Included(ref start) => Bound::Included(start),
            Bound::Excluded(ref start) => Bound::Excluded(start),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&T> {
        match self.end {
            Bound::Included(ref end) => Bound::Included(end),
            Bound::Excluded(ref end) => Bound::Excluded(end),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

impl Type<ClickHouse> for ClickHouseRange<i32> {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INT4_RANGE
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        range_compatible::<i32>(ty)
    }
}

impl Type<ClickHouse> for ClickHouseRange<i64> {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INT8_RANGE
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        range_compatible::<i64>(ty)
    }
}

#[cfg(feature = "bigdecimal")]
impl Type<ClickHouse> for ClickHouseRange<bigdecimal::BigDecimal> {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::NUM_RANGE
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        range_compatible::<bigdecimal::BigDecimal>(ty)
    }
}

#[cfg(feature = "rust_decimal")]
impl Type<ClickHouse> for ClickHouseRange<rust_decimal::Decimal> {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::NUM_RANGE
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        range_compatible::<rust_decimal::Decimal>(ty)
    }
}

#[cfg(feature = "chrono")]
impl Type<ClickHouse> for ClickHouseRange<chrono::NaiveDate> {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::DATE_RANGE
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        range_compatible::<chrono::NaiveDate>(ty)
    }
}

#[cfg(feature = "chrono")]
impl Type<ClickHouse> for ClickHouseRange<chrono::NaiveDateTime> {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TS_RANGE
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        range_compatible::<chrono::NaiveDateTime>(ty)
    }
}

#[cfg(feature = "chrono")]
impl<Tz: chrono::TimeZone> Type<ClickHouse> for ClickHouseRange<chrono::DateTime<Tz>> {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TSTZ_RANGE
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        range_compatible::<chrono::DateTime<Tz>>(ty)
    }
}

#[cfg(feature = "time")]
impl Type<ClickHouse> for ClickHouseRange<time::Date> {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::DATE_RANGE
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        range_compatible::<time::Date>(ty)
    }
}

#[cfg(feature = "time")]
impl Type<ClickHouse> for ClickHouseRange<time::PrimitiveDateTime> {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TS_RANGE
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        range_compatible::<time::PrimitiveDateTime>(ty)
    }
}

#[cfg(feature = "time")]
impl Type<ClickHouse> for ClickHouseRange<time::OffsetDateTime> {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TSTZ_RANGE
    }

    fn compatible(ty: &ClickHouseTypeInfo) -> bool {
        range_compatible::<time::OffsetDateTime>(ty)
    }
}

impl ClickHouseHasArrayType for ClickHouseRange<i32> {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INT4_RANGE_ARRAY
    }
}

impl ClickHouseHasArrayType for ClickHouseRange<i64> {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::INT8_RANGE_ARRAY
    }
}

#[cfg(feature = "bigdecimal")]
impl ClickHouseHasArrayType for ClickHouseRange<bigdecimal::BigDecimal> {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::NUM_RANGE_ARRAY
    }
}

#[cfg(feature = "rust_decimal")]
impl ClickHouseHasArrayType for ClickHouseRange<rust_decimal::Decimal> {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::NUM_RANGE_ARRAY
    }
}

#[cfg(feature = "chrono")]
impl ClickHouseHasArrayType for ClickHouseRange<chrono::NaiveDate> {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::DATE_RANGE_ARRAY
    }
}

#[cfg(feature = "chrono")]
impl ClickHouseHasArrayType for ClickHouseRange<chrono::NaiveDateTime> {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TS_RANGE_ARRAY
    }
}

#[cfg(feature = "chrono")]
impl<Tz: chrono::TimeZone> ClickHouseHasArrayType for ClickHouseRange<chrono::DateTime<Tz>> {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TSTZ_RANGE_ARRAY
    }
}

#[cfg(feature = "time")]
impl ClickHouseHasArrayType for ClickHouseRange<time::Date> {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::DATE_RANGE_ARRAY
    }
}

#[cfg(feature = "time")]
impl ClickHouseHasArrayType for ClickHouseRange<time::PrimitiveDateTime> {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TS_RANGE_ARRAY
    }
}

#[cfg(feature = "time")]
impl ClickHouseHasArrayType for ClickHouseRange<time::OffsetDateTime> {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::TSTZ_RANGE_ARRAY
    }
}

impl<'q, T> Encode<'q, ClickHouse> for ClickHouseRange<T>
where
    T: Encode<'q, ClickHouse>,
{
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        // https://github.com/postgres/postgres/blob/2f48ede080f42b97b594fb14102c82ca1001b80c/src/backend/utils/adt/rangetypes.c#L245

        let mut flags = RangeFlags::empty();

        flags |= match self.start {
            Bound::Included(_) => RangeFlags::LB_INC,
            Bound::Unbounded => RangeFlags::LB_INF,
            Bound::Excluded(_) => RangeFlags::empty(),
        };

        flags |= match self.end {
            Bound::Included(_) => RangeFlags::UB_INC,
            Bound::Unbounded => RangeFlags::UB_INF,
            Bound::Excluded(_) => RangeFlags::empty(),
        };

        buf.push(flags.bits());

        if let Bound::Included(v) | Bound::Excluded(v) = &self.start {
            buf.encode(v)?;
        }

        if let Bound::Included(v) | Bound::Excluded(v) = &self.end {
            buf.encode(v)?;
        }

        // ranges are themselves never null
        Ok(IsNull::No)
    }
}

impl<'r, T> Decode<'r, ClickHouse> for ClickHouseRange<T>
where
    T: Type<ClickHouse> + for<'a> Decode<'a, ClickHouse>,
{
    fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
        match value.format {
            ClickHouseValueFormat::Binary => {
                let element_ty = if let ClickHouseTypeKind::Range(element) = &value.type_info.0.kind() {
                    element
                } else {
                    return Err(format!("unexpected non-range type {}", value.type_info).into());
                };

                let mut buf = value.as_bytes()?;

                let mut start = Bound::Unbounded;
                let mut end = Bound::Unbounded;

                let flags = RangeFlags::from_bits_truncate(buf.get_u8());

                if flags.contains(RangeFlags::EMPTY) {
                    return Ok(ClickHouseRange { start, end });
                }

                if !flags.contains(RangeFlags::LB_INF) {
                    let value =
                        T::decode(ClickHouseValueRef::get(&mut buf, value.format, element_ty.clone())?)?;

                    start = if flags.contains(RangeFlags::LB_INC) {
                        Bound::Included(value)
                    } else {
                        Bound::Excluded(value)
                    };
                }

                if !flags.contains(RangeFlags::UB_INF) {
                    let value =
                        T::decode(ClickHouseValueRef::get(&mut buf, value.format, element_ty.clone())?)?;

                    end = if flags.contains(RangeFlags::UB_INC) {
                        Bound::Included(value)
                    } else {
                        Bound::Excluded(value)
                    };
                }

                Ok(ClickHouseRange { start, end })
            }

            ClickHouseValueFormat::Text => {
                // https://github.com/postgres/postgres/blob/2f48ede080f42b97b594fb14102c82ca1001b80c/src/backend/utils/adt/rangetypes.c#L2046

                let mut start = None;
                let mut end = None;

                let s = value.as_str()?;

                // remember the bounds
                let sb = s.as_bytes();
                let lower = sb[0] as char;
                let upper = sb[sb.len() - 1] as char;

                // trim the wrapping braces/brackets
                let s = &s[1..(s.len() - 1)];

                let mut chars = s.chars();

                let mut element = String::new();
                let mut done = false;
                let mut quoted = false;
                let mut in_quotes = false;
                let mut in_escape = false;
                let mut prev_ch = '\0';
                let mut count = 0;

                while !done {
                    element.clear();

                    loop {
                        match chars.next() {
                            Some(ch) => {
                                match ch {
                                    _ if in_escape => {
                                        element.push(ch);
                                        in_escape = false;
                                    }

                                    '"' if in_quotes => {
                                        in_quotes = false;
                                    }

                                    '"' => {
                                        in_quotes = true;
                                        quoted = true;

                                        if prev_ch == '"' {
                                            element.push('"')
                                        }
                                    }

                                    '\\' if !in_escape => {
                                        in_escape = true;
                                    }

                                    ',' if !in_quotes => break,

                                    _ => {
                                        element.push(ch);
                                    }
                                }
                                prev_ch = ch;
                            }

                            None => {
                                done = true;
                                break;
                            }
                        }
                    }

                    count += 1;
                    if !element.is_empty() || quoted {
                        let value = Some(T::decode(ClickHouseValueRef {
                            type_info: T::type_info(),
                            format: ClickHouseValueFormat::Text,
                            value: Some(element.as_bytes()),
                            row: None,
                        })?);

                        if count == 1 {
                            start = value;
                        } else if count == 2 {
                            end = value;
                        } else {
                            return Err("more than 2 elements found in a range".into());
                        }
                    }
                }

                let start = parse_bound(lower, start)?;
                let end = parse_bound(upper, end)?;

                Ok(ClickHouseRange { start, end })
            }
        }
    }
}

fn parse_bound<T>(ch: char, value: Option<T>) -> Result<Bound<T>, BoxDynError> {
    Ok(if let Some(value) = value {
        match ch {
            '(' | ')' => Bound::Excluded(value),
            '[' | ']' => Bound::Included(value),

            _ => {
                return Err(format!(
                    "expected `(`, ')', '[', or `]` but found `{ch}` for range literal"
                )
                .into());
            }
        }
    } else {
        Bound::Unbounded
    })
}

impl<T> Display for ClickHouseRange<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.start {
            Bound::Unbounded => f.write_str("(,")?,
            Bound::Excluded(v) => write!(f, "({v},")?,
            Bound::Included(v) => write!(f, "[{v},")?,
        }

        match &self.end {
            Bound::Unbounded => f.write_str(")")?,
            Bound::Excluded(v) => write!(f, "{v})")?,
            Bound::Included(v) => write!(f, "{v}]")?,
        }

        Ok(())
    }
}

fn range_compatible<E: Type<ClickHouse>>(ty: &ClickHouseTypeInfo) -> bool {
    // we require the declared type to be a _range_ with an
    // element type that is acceptable
    if let ClickHouseTypeKind::Range(element) = &ty.kind() {
        return E::compatible(element);
    }

    false
}
