use crate::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
    {ClickHouseArgumentBuffer, ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueFormat, ClickHouseValueRef, ClickHouse},
};
use byteorder::{BigEndian, ByteOrder};
use std::{
    io,
    ops::{Add, AddAssign, Sub, SubAssign},
};

/// The ClickHouse [`MONEY`] type stores a currency amount with a fixed fractional
/// precision. The fractional precision is determined by the database's
/// `lc_monetary` setting.
///
/// Data is read and written as 64-bit signed integers, and conversion into a
/// decimal should be done using the right precision.
///
/// Reading `MONEY` value in text format is not supported and will cause an error.
///
/// ### `locale_frac_digits`
/// This parameter corresponds to the number of digits after the decimal separator.
///
/// This value must match what ClickHouse is expecting for the locale set in the database
/// or else the decimal value you see on the client side will not match the `money` value
/// on the server side.
///
/// **For _most_ locales, this value is `2`.**
///
/// If you're not sure what locale your database is set to or how many decimal digits it specifies,
/// you can execute `SHOW lc_monetary;` to get the locale name, and then look it up in this list
/// (you can ignore the `.utf8` prefix):
/// <https://lh.2xlibre.net/values/frac_digits/>
///
/// If that link is dead and you're on a POSIX-compliant system (Unix, FreeBSD) you can also execute:
///
/// ```sh
/// $ LC_MONETARY=<value returned by `SHOW lc_monetary`> locale -k frac_digits
/// ```
///
/// And the value you want is `N` in `frac_digits=N`. If you have shell access to the database
/// server you should execute it there as available locales may differ between machines.
///
/// Note that if `frac_digits` for the locale is outside the range `[0, 10]`, ClickHouse assumes
/// it's a sentinel value and defaults to 2:
/// <https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/cash.c#L114-L123>
///
/// [`MONEY`]: https://www.postgresql.org/docs/current/datatype-money.html
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub struct ClickHouseMoney(
    /// The raw integer value sent over the wire; for locales with `frac_digits=2` (i.e. most
    /// of them), this will be the value in whole cents.
    ///
    /// E.g. for `select '$123.45'::money` with a locale of `en_US` (`frac_digits=2`),
    /// this will be `12345`.
    ///
    /// If the currency of your locale does not have fractional units, e.g. Yen, then this will
    /// just be the units of the currency.
    ///
    /// See the type-level docs for an explanation of `locale_frac_units`.
    pub i64,
);

impl ClickHouseMoney {
    /// Convert the money value into a [`BigDecimal`] using `locale_frac_digits`.
    ///
    /// See the type-level docs for an explanation of `locale_frac_digits`.
    ///
    /// [`BigDecimal`]: bigdecimal::BigDecimal
    #[cfg(feature = "bigdecimal")]
    pub fn to_bigdecimal(self, locale_frac_digits: i64) -> bigdecimal::BigDecimal {
        let digits = num_bigint::BigInt::from(self.0);

        bigdecimal::BigDecimal::new(digits, locale_frac_digits)
    }

    /// Convert the money value into a [`Decimal`] using `locale_frac_digits`.
    ///
    /// See the type-level docs for an explanation of `locale_frac_digits`.
    ///
    /// [`Decimal`]: rust_decimal::Decimal
    #[cfg(feature = "rust_decimal")]
    pub fn to_decimal(self, locale_frac_digits: u32) -> rust_decimal::Decimal {
        rust_decimal::Decimal::new(self.0, locale_frac_digits)
    }

    /// Convert a [`Decimal`] value into money using `locale_frac_digits`.
    ///
    /// See the type-level docs for an explanation of `locale_frac_digits`.
    ///
    /// Note that `Decimal` has 96 bits of precision, but `ClickHouseMoney` only has 63 plus the sign bit.
    /// If the value is larger than 63 bits it will be truncated.
    ///
    /// [`Decimal`]: rust_decimal::Decimal
    #[cfg(feature = "rust_decimal")]
    pub fn from_decimal(mut decimal: rust_decimal::Decimal, locale_frac_digits: u32) -> Self {
        // this is all we need to convert to our expected locale's `frac_digits`
        decimal.rescale(locale_frac_digits);

        /// a mask to bitwise-AND with an `i64` to zero the sign bit
        const SIGN_MASK: i64 = i64::MAX;

        let is_negative = decimal.is_sign_negative();
        let serialized = decimal.serialize();

        // interpret bytes `4..12` as an i64, ignoring the sign bit
        // this is where truncation occurs
        let value = i64::from_le_bytes(
            *<&[u8; 8]>::try_from(&serialized[4..12])
                .expect("BUG: slice of serialized should be 8 bytes"),
        ) & SIGN_MASK; // zero out the sign bit

        // negate if necessary
        Self(if is_negative { -value } else { value })
    }

    /// Convert a [`BigDecimal`](bigdecimal::BigDecimal) value into money using the correct precision
    /// defined in the ClickHouse settings. The default precision is two.
    #[cfg(feature = "bigdecimal")]
    pub fn from_bigdecimal(
        decimal: bigdecimal::BigDecimal,
        locale_frac_digits: u32,
    ) -> Result<Self, BoxDynError> {
        use bigdecimal::ToPrimitive;

        let multiplier = bigdecimal::BigDecimal::new(
            num_bigint::BigInt::from(10i128.pow(locale_frac_digits)),
            0,
        );

        let cents = decimal * multiplier;

        let money = cents.to_i64().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Provided BigDecimal could not convert to i64: overflow.",
            )
        })?;

        Ok(Self(money))
    }
}

impl Type<ClickHouse> for ClickHouseMoney {
    fn type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::MONEY
    }
}

impl ClickHouseHasArrayType for ClickHouseMoney {
    fn array_type_info() -> ClickHouseTypeInfo {
        ClickHouseTypeInfo::MONEY_ARRAY
    }
}

impl<T> From<T> for ClickHouseMoney
where
    T: Into<i64>,
{
    fn from(num: T) -> Self {
        Self(num.into())
    }
}

impl Encode<'_, ClickHouse> for ClickHouseMoney {
    fn encode_by_ref(&self, buf: &mut ClickHouseArgumentBuffer) -> Result<IsNull, BoxDynError> {
        buf.extend(&self.0.to_be_bytes());

        Ok(IsNull::No)
    }
}

impl Decode<'_, ClickHouse> for ClickHouseMoney {
    fn decode(value: ClickHouseValueRef<'_>) -> Result<Self, BoxDynError> {
        match value.format() {
            ClickHouseValueFormat::Binary => {
                let cents = BigEndian::read_i64(value.as_bytes()?);

                Ok(ClickHouseMoney(cents))
            }
            ClickHouseValueFormat::Text => {
                let error = io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Reading a `MONEY` value in text format is not supported.",
                );

                Err(Box::new(error))
            }
        }
    }
}

impl Add<ClickHouseMoney> for ClickHouseMoney {
    type Output = ClickHouseMoney;

    /// Adds two monetary values.
    ///
    /// # Panics
    /// Panics if overflowing the `i64::MAX`.
    fn add(self, rhs: ClickHouseMoney) -> Self::Output {
        self.0
            .checked_add(rhs.0)
            .map(ClickHouseMoney)
            .expect("overflow adding money amounts")
    }
}

impl AddAssign<ClickHouseMoney> for ClickHouseMoney {
    /// An assigning add for two monetary values.
    ///
    /// # Panics
    /// Panics if overflowing the `i64::MAX`.
    fn add_assign(&mut self, rhs: ClickHouseMoney) {
        self.0 = self
            .0
            .checked_add(rhs.0)
            .expect("overflow adding money amounts")
    }
}

impl Sub<ClickHouseMoney> for ClickHouseMoney {
    type Output = ClickHouseMoney;

    /// Subtracts two monetary values.
    ///
    /// # Panics
    /// Panics if underflowing the `i64::MIN`.
    fn sub(self, rhs: ClickHouseMoney) -> Self::Output {
        self.0
            .checked_sub(rhs.0)
            .map(ClickHouseMoney)
            .expect("overflow subtracting money amounts")
    }
}

impl SubAssign<ClickHouseMoney> for ClickHouseMoney {
    /// An assigning subtract for two monetary values.
    ///
    /// # Panics
    /// Panics if underflowing the `i64::MIN`.
    fn sub_assign(&mut self, rhs: ClickHouseMoney) {
        self.0 = self
            .0
            .checked_sub(rhs.0)
            .expect("overflow subtracting money amounts")
    }
}

#[cfg(test)]
mod tests {
    use super::ClickHouseMoney;

    #[test]
    fn adding_works() {
        assert_eq!(ClickHouseMoney(3), ClickHouseMoney(1) + ClickHouseMoney(2))
    }

    #[test]
    fn add_assign_works() {
        let mut money = ClickHouseMoney(1);
        money += ClickHouseMoney(2);

        assert_eq!(ClickHouseMoney(3), money);
    }

    #[test]
    fn subtracting_works() {
        assert_eq!(ClickHouseMoney(4), ClickHouseMoney(5) - ClickHouseMoney(1))
    }

    #[test]
    fn sub_assign_works() {
        let mut money = ClickHouseMoney(1);
        money -= ClickHouseMoney(2);

        assert_eq!(ClickHouseMoney(-1), money);
    }

    #[test]
    fn default_value() {
        let money = ClickHouseMoney::default();

        assert_eq!(money, ClickHouseMoney(0));
    }

    #[test]
    #[should_panic]
    fn add_overflow_panics() {
        let _ = ClickHouseMoney(i64::MAX) + ClickHouseMoney(1);
    }

    #[test]
    #[should_panic]
    fn add_assign_overflow_panics() {
        let mut money = ClickHouseMoney(i64::MAX);
        money += ClickHouseMoney(1);
    }

    #[test]
    #[should_panic]
    fn sub_overflow_panics() {
        let _ = ClickHouseMoney(i64::MIN) - ClickHouseMoney(1);
    }

    #[test]
    #[should_panic]
    fn sub_assign_overflow_panics() {
        let mut money = ClickHouseMoney(i64::MIN);
        money -= ClickHouseMoney(1);
    }

    #[test]
    #[cfg(feature = "bigdecimal")]
    fn conversion_to_bigdecimal_works() {
        let money = ClickHouseMoney(12345);

        assert_eq!(
            bigdecimal::BigDecimal::new(num_bigint::BigInt::from(12345), 2),
            money.to_bigdecimal(2)
        );
    }

    #[test]
    #[cfg(feature = "rust_decimal")]
    fn conversion_to_decimal_works() {
        assert_eq!(
            rust_decimal::Decimal::new(12345, 2),
            ClickHouseMoney(12345).to_decimal(2)
        );
    }

    #[test]
    #[cfg(feature = "rust_decimal")]
    fn conversion_from_decimal_works() {
        assert_eq!(
            ClickHouseMoney(12345),
            ClickHouseMoney::from_decimal(rust_decimal::Decimal::new(12345, 2), 2)
        );

        assert_eq!(
            ClickHouseMoney(12345),
            ClickHouseMoney::from_decimal(rust_decimal::Decimal::new(123450, 3), 2)
        );

        assert_eq!(
            ClickHouseMoney(-12345),
            ClickHouseMoney::from_decimal(rust_decimal::Decimal::new(-123450, 3), 2)
        );

        assert_eq!(
            ClickHouseMoney(-12300),
            ClickHouseMoney::from_decimal(rust_decimal::Decimal::new(-123, 0), 2)
        );
    }

    #[test]
    #[cfg(feature = "bigdecimal")]
    fn conversion_from_bigdecimal_works() {
        let dec = bigdecimal::BigDecimal::new(num_bigint::BigInt::from(12345), 2);

        assert_eq!(ClickHouseMoney(12345), ClickHouseMoney::from_bigdecimal(dec, 2).unwrap());
    }
}
