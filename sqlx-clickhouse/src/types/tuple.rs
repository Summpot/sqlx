use crate::decode::Decode;
use crate::error::BoxDynError;
use crate::types::ClickHouseRecordDecoder;
use crate::types::Type;
use crate::{ClickHouseHasArrayType, ClickHouseTypeInfo, ClickHouseValueRef, ClickHouse};

macro_rules! impl_type_for_tuple {
    ($( $idx:ident : $T:ident ),*) => {
        impl<$($T,)*> Type<ClickHouse> for ($($T,)*) {


            #[inline]
            fn type_info() -> ClickHouseTypeInfo {
                ClickHouseTypeInfo::RECORD
            }
        }

        impl<$($T,)*> ClickHouseHasArrayType for ($($T,)*) {


            #[inline]
            fn array_type_info() -> ClickHouseTypeInfo {
                ClickHouseTypeInfo::RECORD_ARRAY
            }
        }

        impl<'r, $($T,)*> Decode<'r, ClickHouse> for ($($T,)*)
        where
            $($T: 'r,)*
            $($T: Type<ClickHouse>,)*
            $($T: for<'a> Decode<'a, ClickHouse>,)*
        {


            fn decode(value: ClickHouseValueRef<'r>) -> Result<Self, BoxDynError> {
                #[allow(unused)]
                let mut decoder = ClickHouseRecordDecoder::new(value)?;

                $(let $idx: $T = decoder.try_decode()?;)*

                Ok(($($idx,)*))
            }
        }
    };
}

impl_type_for_tuple!(_1: T1);

impl_type_for_tuple!(_1: T1, _2: T2);

impl_type_for_tuple!(_1: T1, _2: T2, _3: T3);

impl_type_for_tuple!(_1: T1, _2: T2, _3: T3, _4: T4);

impl_type_for_tuple!(_1: T1, _2: T2, _3: T3, _4: T4, _5: T5);

impl_type_for_tuple!(_1: T1, _2: T2, _3: T3, _4: T4, _5: T5, _6: T6);

impl_type_for_tuple!(_1: T1, _2: T2, _3: T3, _4: T4, _5: T5, _6: T6, _7: T7);

impl_type_for_tuple!(
    _1: T1,
    _2: T2,
    _3: T3,
    _4: T4,
    _5: T5,
    _6: T6,
    _7: T7,
    _8: T8
);

impl_type_for_tuple!(
    _1: T1,
    _2: T2,
    _3: T3,
    _4: T4,
    _5: T5,
    _6: T6,
    _7: T7,
    _8: T8,
    _9: T9
);
