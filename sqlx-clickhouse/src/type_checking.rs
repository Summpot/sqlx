use crate::ClickHouse;

// The paths used below will also be emitted by the macros so they have to match the final facade.
#[allow(unused_imports, dead_code)]
mod sqlx {
    pub use crate as postgres;
    pub use sqlx_core::*;
}

impl_type_checking!(
        ClickHouse {
        (),
        bool,
        String | &str,
        i8,
        i16,
        i32,
        i64,
        f32,
        f64,
        Vec<u8> | &[u8],

        sqlx::postgres::types::Oid,

        sqlx::postgres::types::ClickHouseInterval,

        sqlx::postgres::types::ClickHouseMoney,

        sqlx::postgres::types::ClickHouseLTree,

        sqlx::postgres::types::ClickHouseLQuery,

        sqlx::postgres::types::ClickHouseCube,

        sqlx::postgres::types::ClickHousePoint,

        sqlx::postgres::types::ClickHouseLine,

        sqlx::postgres::types::ClickHouseLSeg,

        sqlx::postgres::types::ClickHouseBox,

        #[cfg(feature = "uuid")]
        sqlx::types::Uuid,

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        sqlx::types::chrono::NaiveTime,

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        sqlx::types::chrono::NaiveDate,

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        sqlx::types::chrono::NaiveDateTime,

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc> | sqlx::types::chrono::DateTime<_>,

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        sqlx::postgres::types::ClickHouseTimeTz<sqlx::types::chrono::NaiveTime, sqlx::types::chrono::FixedOffset>,

        #[cfg(feature = "time")]
        sqlx::types::time::Time,

        #[cfg(feature = "time")]
        sqlx::types::time::Date,

        #[cfg(feature = "time")]
        sqlx::types::time::PrimitiveDateTime,

        #[cfg(feature = "time")]
        sqlx::types::time::OffsetDateTime,

        #[cfg(feature = "time")]
        sqlx::postgres::types::ClickHouseTimeTz<sqlx::types::time::Time, sqlx::types::time::UtcOffset>,

        #[cfg(feature = "bigdecimal")]
        sqlx::types::BigDecimal,

        #[cfg(feature = "rust_decimal")]
        sqlx::types::Decimal,

        #[cfg(feature = "ipnetwork")]
        sqlx::types::ipnetwork::IpNetwork,

        #[cfg(feature = "mac_address")]
        sqlx::types::mac_address::MacAddress,

        #[cfg(feature = "json")]
        sqlx::types::JsonValue,

        #[cfg(feature = "bit-vec")]
        sqlx::types::BitVec,

        sqlx::postgres::types::ClickHouseHstore,
        // Arrays

        Vec<bool> | &[bool],
        Vec<String> | &[String],
        Vec<Vec<u8>> | &[Vec<u8>],
        Vec<i8> | &[i8],
        Vec<i16> | &[i16],
        Vec<i32> | &[i32],
        Vec<i64> | &[i64],
        Vec<f32> | &[f32],
        Vec<f64> | &[f64],
        Vec<sqlx::postgres::types::Oid> | &[sqlx::postgres::types::Oid],
        Vec<sqlx::postgres::types::ClickHouseMoney> | &[sqlx::postgres::types::ClickHouseMoney],
        Vec<sqlx::postgres::types::ClickHouseInterval> | &[sqlx::postgres::types::ClickHouseInterval],

        #[cfg(feature = "uuid")]
        Vec<sqlx::types::Uuid> | &[sqlx::types::Uuid],

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        Vec<sqlx::types::chrono::NaiveTime> | &[sqlx::types::chrono::NaiveTime],

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        Vec<sqlx::types::chrono::NaiveDate> | &[sqlx::types::chrono::NaiveDate],

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        Vec<sqlx::types::chrono::NaiveDateTime> | &[sqlx::types::chrono::NaiveDateTime],

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        Vec<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>> | &[sqlx::types::chrono::DateTime<_>],

        #[cfg(feature = "time")]
        Vec<sqlx::types::time::Time> | &[sqlx::types::time::Time],

        #[cfg(feature = "time")]
        Vec<sqlx::types::time::Date> | &[sqlx::types::time::Date],

        #[cfg(feature = "time")]
        Vec<sqlx::types::time::PrimitiveDateTime> | &[sqlx::types::time::PrimitiveDateTime],

        #[cfg(feature = "time")]
        Vec<sqlx::types::time::OffsetDateTime> | &[sqlx::types::time::OffsetDateTime],

        #[cfg(feature = "bigdecimal")]
        Vec<sqlx::types::BigDecimal> | &[sqlx::types::BigDecimal],

        #[cfg(feature = "rust_decimal")]
        Vec<sqlx::types::Decimal> | &[sqlx::types::Decimal],

        #[cfg(feature = "ipnetwork")]
        Vec<sqlx::types::ipnetwork::IpNetwork> | &[sqlx::types::ipnetwork::IpNetwork],

        #[cfg(feature = "mac_address")]
        Vec<sqlx::types::mac_address::MacAddress> | &[sqlx::types::mac_address::MacAddress],

        #[cfg(feature = "json")]
        Vec<sqlx::types::JsonValue> | &[sqlx::types::JsonValue],

        Vec<sqlx::postgres::types::ClickHouseHstore> | &[sqlx::postgres::types::ClickHouseHstore],

        // Ranges

        sqlx::postgres::types::ClickHouseRange<i32>,
        sqlx::postgres::types::ClickHouseRange<i64>,

        #[cfg(feature = "bigdecimal")]
        sqlx::postgres::types::ClickHouseRange<sqlx::types::BigDecimal>,

        #[cfg(feature = "rust_decimal")]
        sqlx::postgres::types::ClickHouseRange<sqlx::types::Decimal>,

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::NaiveDate>,

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::NaiveDateTime>,

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>> |
            sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::DateTime<_>>,

        #[cfg(feature = "time")]
        sqlx::postgres::types::ClickHouseRange<sqlx::types::time::Date>,

        #[cfg(feature = "time")]
        sqlx::postgres::types::ClickHouseRange<sqlx::types::time::PrimitiveDateTime>,

        #[cfg(feature = "time")]
        sqlx::postgres::types::ClickHouseRange<sqlx::types::time::OffsetDateTime>,

        // Range arrays

        Vec<sqlx::postgres::types::ClickHouseRange<i32>> | &[sqlx::postgres::types::ClickHouseRange<i32>],
        Vec<sqlx::postgres::types::ClickHouseRange<i64>> | &[sqlx::postgres::types::ClickHouseRange<i64>],

        #[cfg(feature = "bigdecimal")]
        Vec<sqlx::postgres::types::ClickHouseRange<sqlx::types::BigDecimal>> |
            &[sqlx::postgres::types::ClickHouseRange<sqlx::types::BigDecimal>],

        #[cfg(feature = "rust_decimal")]
        Vec<sqlx::postgres::types::ClickHouseRange<sqlx::types::Decimal>> |
            &[sqlx::postgres::types::ClickHouseRange<sqlx::types::Decimal>],

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        Vec<sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::NaiveDate>> |
            &[sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::NaiveDate>],

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        Vec<sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::NaiveDateTime>> |
            &[sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::NaiveDateTime>],

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        Vec<sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>> |
            &[sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::DateTime<_>>],

        #[cfg(all(feature = "chrono", not(feature = "time")))]
        Vec<sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>> |
            &[sqlx::postgres::types::ClickHouseRange<sqlx::types::chrono::DateTime<_>>],

        #[cfg(feature = "time")]
        Vec<sqlx::postgres::types::ClickHouseRange<sqlx::types::time::Date>> |
            &[sqlx::postgres::types::ClickHouseRange<sqlx::types::time::Date>],

        #[cfg(feature = "time")]
        Vec<sqlx::postgres::types::ClickHouseRange<sqlx::types::time::PrimitiveDateTime>> |
            &[sqlx::postgres::types::ClickHouseRange<sqlx::types::time::PrimitiveDateTime>],

        #[cfg(feature = "time")]
        Vec<sqlx::postgres::types::ClickHouseRange<sqlx::types::time::OffsetDateTime>> |
            &[sqlx::postgres::types::ClickHouseRange<sqlx::types::time::OffsetDateTime>],
    },
    ParamChecking::Strong,
    feature-types: info => info.__type_feature_gate(),
);
