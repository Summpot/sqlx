use bigdecimal::BigDecimal;
use num_bigint::{BigInt, Sign};
use std::cmp;

use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::numeric::{ClickHouseNumeric, ClickHouseNumericSign};
use crate::types::Type;
use crate::{ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse};

impl Type<ClickHouse> for BigDecimal {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::NUMERIC
    }
}

impl ClickHouseHasArrayType for BigDecimal {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::NUMERIC_ARRAY
    }
}

impl TryFrom<ClickHouseNumeric> for BigDecimal {
    type Error = BoxDynError;

    fn try_from(numeric: ClickHouseNumeric) -> Result<Self, BoxDynError> {
        Self::try_from(&numeric)
    }
}

impl TryFrom<&'_ ClickHouseNumeric> for BigDecimal {
    type Error = BoxDynError;

    fn try_from(numeric: &'_ ClickHouseNumeric) -> Result<Self, Self::Error> {
        let (digits, sign, weight) = match *numeric {
            ClickHouseNumeric::Number {
                ref digits,
                sign,
                weight,
                ..
            } => (digits, sign, weight),

            ClickHouseNumeric::NotANumber => {
                return Err("BigDecimal does not support NaN values".into());
            }
        };

        if digits.is_empty() {
            // ClickHouse returns an empty digit array for 0 but BigInt expects at least one zero
            return Ok(0u64.into());
        }

        let sign = match sign {
            ClickHouseNumericSign::Positive => Sign::Plus,
            ClickHouseNumericSign::Negative => Sign::Minus,
        };

        // weight is 0 if the decimal point falls after the first base-10000 digit
        //
        // `Vec` capacity cannot exceed `isize::MAX` bytes, so this cast can't wrap in practice.
        #[allow(clippy::cast_possible_wrap)]
        let scale = (digits.len() as i64 - weight as i64 - 1) * 4;

        // no optimized algorithm for base-10 so use base-100 for faster processing
        let mut cents = Vec::with_capacity(digits.len() * 2);

        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_possible_wrap,
            clippy::cast_sign_loss
        )]
        for (i, &digit) in digits.iter().enumerate() {
            if !ClickHouseNumeric::is_valid_digit(digit) {
                return Err(format!(
                    "ClickHouseNumeric to BigDecimal: {i}th digit is out of range {digit}"
                )
                .into());
            }

            cents.push((digit / 100) as u8);
            cents.push((digit % 100) as u8);
        }

        let bigint = BigInt::from_radix_be(sign, &cents, 100)
            .ok_or("ClickHouseNumeric contained an out-of-range digit")?;

        Ok(BigDecimal::new(bigint, scale))
    }
}

impl TryFrom<&'_ BigDecimal> for ClickHouseNumeric {
    type Error = BoxDynError;

    fn try_from(decimal: &BigDecimal) -> Result<Self, BoxDynError> {
        let base_10_to_10000 = |chunk: &[u8]| chunk.iter().fold(0i16, |a, &d| a * 10 + d as i16);

        // NOTE: this unfortunately copies the BigInt internally
        let (integer, exp) = decimal.as_bigint_and_exponent();

        // this routine is specifically optimized for base-10
        // FIXME: is there a way to iterate over the digits to avoid the Vec allocation
        let (sign, base_10) = integer.to_radix_be(10);

        let base_10_len = i64::try_from(base_10.len()).map_err(|_| {
            format!(
                "BigDecimal base-10 length out of range for ClickHouseNumeric: {}",
                base_10.len()
            )
        })?;

        // weight is positive power of 10000
        // exp is the negative power of 10
        let weight_10 = base_10_len - exp;

        // scale is only nonzero when we have fractional digits
        // since `exp` is the _negative_ decimal exponent, it tells us
        // exactly what our scale should be
        let scale: i16 = cmp::max(0, exp).try_into()?;

        // there's an implicit +1 offset in the interpretation
        let weight: i16 = if weight_10 <= 0 {
            weight_10 / 4 - 1
        } else {
            // the `-1` is a fix for an off by 1 error (4 digits should still be 0 weight)
            (weight_10 - 1) / 4
        }
        .try_into()?;

        let digits_len = if base_10.len() % 4 != 0 {
            base_10.len() / 4 + 1
        } else {
            base_10.len() / 4
        };

        // For efficiency, we want to process the base-10 digits in chunks of 4,
        // but that means we need to deal with the non-divisible remainder first.
        let offset = weight_10.rem_euclid(4);

        // Do a checked conversion to the smallest integer,
        // so we can widen arbitrarily without triggering lints.
        let offset = u8::try_from(offset).unwrap_or_else(|_| {
            panic!("BUG: `offset` should be in the range [0, 4) but is {offset}")
        });

        let mut digits = Vec::with_capacity(digits_len);

        if let Some(first) = base_10.get(..offset as usize) {
            if !first.is_empty() {
                digits.push(base_10_to_10000(first));
            }
        } else if offset != 0 {
            // If we didn't hit the `if let Some` branch,
            // then `base_10.len()` must strictly be smaller
            #[allow(clippy::cast_possible_truncation)]
            let power = (offset as usize - base_10.len()) as u32;

            digits.push(base_10_to_10000(&base_10) * 10i16.pow(power));
        }

        if let Some(rest) = base_10.get(offset as usize..) {
            // `chunk.len()` is always between 1 and 4
            #[allow(clippy::cast_possible_truncation)]
            digits.extend(
                rest.chunks(4)
                    .map(|chunk| base_10_to_10000(chunk) * 10i16.pow(4 - chunk.len() as u32)),
            );
        }

        while let Some(&0) = digits.last() {
            digits.pop();
        }

        Ok(ClickHouseNumeric::Number {
            sign: sign_to_pg(sign),
            scale,
            weight,
            digits,
        })
    }
}

#[doc=include_str!("bigdecimal-range.md")]
impl Encode<'_, ClickHouse> for BigDecimal {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        ClickHouseNumeric::try_from(self)?.encode(buf)?;

        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        ClickHouseNumeric::size_hint(self.digits())
    }
}

/// ### Note: `NaN`
/// `BigDecimal` has a greater range than `NUMERIC` (see the corresponding `Encode` impl for details)
/// but cannot represent `NaN`, so decoding may return an error.
impl Decode<'_, ClickHouse> for BigDecimal {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        match value.format() {
            ClickHouseValueFormat::Binary => ClickHouseNumeric::decode(value.as_bytes()?)?.try_into(),
            ClickHouseValueFormat::Text => Ok(value.as_str()?.parse::<BigDecimal>()?),
        }
    }
}

fn sign_to_pg(sign: Sign) -> ClickHouseNumericSign {
    match sign {
        Sign::Plus | Sign::NoSign => ClickHouseNumericSign::Positive,
        Sign::Minus => ClickHouseNumericSign::Negative,
    }
}

#[cfg(test)]
mod bigdecimal_to_pgnumeric {
    use super::{BigDecimal, ClickHouseNumeric, ClickHouseNumericSign};
    use std::convert::TryFrom;

    #[test]
    fn zero() {
        let zero: BigDecimal = "0".parse().unwrap();

        assert_eq!(
            ClickHouseNumeric::try_from(&zero).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 0,
                weight: 0,
                digits: vec![]
            }
        );
    }

    #[test]
    fn one() {
        let one: BigDecimal = "1".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&one).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 0,
                weight: 0,
                digits: vec![1]
            }
        );
    }

    #[test]
    fn ten() {
        let ten: BigDecimal = "10".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&ten).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 0,
                weight: 0,
                digits: vec![10]
            }
        );
    }

    #[test]
    fn one_hundred() {
        let one_hundred: BigDecimal = "100".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&one_hundred).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 0,
                weight: 0,
                digits: vec![100]
            }
        );
    }

    #[test]
    fn ten_thousand() {
        // BigDecimal doesn't normalize here
        let ten_thousand: BigDecimal = "10000".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&ten_thousand).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 0,
                weight: 1,
                digits: vec![1]
            }
        );
    }

    #[test]
    fn two_digits() {
        let two_digits: BigDecimal = "12345".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&two_digits).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 0,
                weight: 1,
                digits: vec![1, 2345]
            }
        );
    }

    #[test]
    fn one_tenth() {
        let one_tenth: BigDecimal = "0.1".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&one_tenth).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 1,
                weight: -1,
                digits: vec![1000]
            }
        );
    }

    #[test]
    fn one_hundredth() {
        let one_hundredth: BigDecimal = "0.01".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&one_hundredth).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 2,
                weight: -1,
                digits: vec![100]
            }
        );
    }

    #[test]
    fn twelve_thousandths() {
        let twelve_thousandths: BigDecimal = "0.012".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&twelve_thousandths).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 3,
                weight: -1,
                digits: vec![120]
            }
        );
    }

    #[test]
    fn decimal_1() {
        let decimal: BigDecimal = "1.2345".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&decimal).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 4,
                weight: 0,
                digits: vec![1, 2345]
            }
        );
    }

    #[test]
    fn decimal_2() {
        let decimal: BigDecimal = "0.12345".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&decimal).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 5,
                weight: -1,
                digits: vec![1234, 5000]
            }
        );
    }

    #[test]
    fn decimal_3() {
        let decimal: BigDecimal = "0.01234".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&decimal).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 5,
                weight: -1,
                digits: vec![0123, 4000]
            }
        );
    }

    #[test]
    fn decimal_4() {
        let decimal: BigDecimal = "12345.67890".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&decimal).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 5,
                weight: 1,
                digits: vec![1, 2345, 6789]
            }
        );
    }

    #[test]
    fn one_digit_decimal() {
        let one_digit_decimal: BigDecimal = "0.00001234".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&one_digit_decimal).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 8,
                weight: -2,
                digits: vec![1234]
            }
        );
    }

    #[test]
    fn issue_423_four_digit() {
        // This is a regression test for https://github.com/launchbadge/sqlx/issues/423
        let four_digit: BigDecimal = "1234".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&four_digit).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 0,
                weight: 0,
                digits: vec![1234]
            }
        );
    }

    #[test]
    fn issue_423_negative_four_digit() {
        // This is a regression test for https://github.com/launchbadge/sqlx/issues/423
        let negative_four_digit: BigDecimal = "-1234".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&negative_four_digit).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Negative,
                scale: 0,
                weight: 0,
                digits: vec![1234]
            }
        );
    }

    #[test]
    fn issue_423_eight_digit() {
        // This is a regression test for https://github.com/launchbadge/sqlx/issues/423
        let eight_digit: BigDecimal = "12345678".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&eight_digit).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Positive,
                scale: 0,
                weight: 1,
                digits: vec![1234, 5678]
            }
        );
    }

    #[test]
    fn issue_423_negative_eight_digit() {
        // This is a regression test for https://github.com/launchbadge/sqlx/issues/423
        let negative_eight_digit: BigDecimal = "-12345678".parse().unwrap();
        assert_eq!(
            ClickHouseNumeric::try_from(&negative_eight_digit).unwrap(),
            ClickHouseNumeric::Number {
                sign: ClickHouseNumericSign::Negative,
                scale: 0,
                weight: 1,
                digits: vec![1234, 5678]
            }
        );
    }
}
