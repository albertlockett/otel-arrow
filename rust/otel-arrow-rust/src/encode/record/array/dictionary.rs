// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! this module contains base types for enabling dictionary encoding with the adaptive array
//! builder. The adaptive array builder will try to use the smallest dictionary index possible
//! when building array using dictionary encoding and will interact with the types in this module
//! to building the dictionary and upgrading the index type on overflows.

use std::sync::Arc;

use arrow::{
    array::{AnyDictionaryArray, Array, ArrayRef, ArrowPrimitiveType, DictionaryArray},
    datatypes::{ArrowDictionaryKeyType, DataType, UInt8Type, UInt16Type},
    error::ArrowError,
};
use snafu::Snafu;

use crate::{
    arrays::NullableArrayAccessor,
    encode::record::array::{ArrayAppend, CheckedArrayAppend},
};

use super::{ArrayBuilder, ArrayBuilderConstructor};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum DictionaryBuilderError {
    #[snafu(display("dict overflow"))]
    DictOverflow {},
}

pub type Result<T> = std::result::Result<T, DictionaryBuilderError>;

// TODO comment about what is happening here ...
pub mod checked {
    use super::*;

    #[derive(Snafu, Debug)]
    #[snafu(visibility(pub))]
    pub enum DictionaryBuilderError {
        #[snafu(display("dict overflow"))]
        DictOverflow {},

        #[snafu(display("checked builder error"))]
        CheckedBuilderError {
            #[snafu(source)]
            source: ArrowError,
        },
    }

    pub type Result<T> = std::result::Result<T, DictionaryBuilderError>;
}

// This is the base trait for array builder implementations that are
// used to construct dictionary arrays
pub trait DictionaryArrayAppend<T>
where
    T: ArrowDictionaryKeyType,
{
    type Native;

    // Append a new value to the dictionary, and return the index of
    // the keys array. The returned index can by AdaptiveDictionaryBuilder
    // to determine if the dictionary overflows.
    //
    // If the implementing builder can determine internally that the dictionary
    // would overflow, it can also return `DictOverflow` error
    fn append_value(&mut self, value: &Self::Native) -> Result<usize>;
}

pub trait CheckedDictionaryArrayAppend<T>
where
    T: ArrowDictionaryKeyType,
{
    type Native;

    fn append_value(&mut self, value: &Self::Native) -> checked::Result<usize>;
}

pub trait DictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    fn finish(&mut self) -> DictionaryArray<K>;
}

// pub struct DictionaryArrayWithType<T>
// where
//     T: ArrowDictionaryKeyType,
// {
//     pub array: Arc<DictionaryArray<T>>,
//     pub data_type: DataType,
// }

impl<T> From<DictionaryArray<T>> for super::ArrayWithType
where
    T: ArrowDictionaryKeyType,
{
    fn from(value: DictionaryArray<T>) -> Self {
        let data_type = value.data_type().clone();
        super::ArrayWithType {
            array: Arc::new(value),
            data_type,
        }
    }
}

// This trait is used to help the AdaptiveDictionaryBuilder to convert the dictionary
// array constructed by the underlying builder to the native type. It will use the
// associated `Accessor` type to downcast the values array, so by implementing this trait
// implementations of `DictionaryArrayBuilder` have a way to signal to
// `AdaptiveDictionaryBuilder` what is the underlying type of the values array.
pub trait ConvertToNativeHelper {
    type Accessor;
}

// Implementations of this trait are used to upgrade from a builder for a dictionary
// keyed by a smaller index type into a larger type. E.g. a builder for
// DictionaryArray<u8> -> DictionaryArray<u16>
pub trait UpdateDictionaryIndexInto<T> {
    fn upgrade_into(&mut self) -> T;
}

pub struct DictionaryOptions {
    pub max_cardinality: u16,
    pub min_cardinality: u16,
    // TODO there's something called reset_threshold in the golang code
    // that maybe we need to add here?
}

enum DictIndexVariant<T8, T16> {
    UInt8(T8),
    UInt16(T16),
}

pub struct AdaptiveDictionaryBuilder<T8, T16> {
    max_cardinality: u16,
    variant: DictIndexVariant<T8, T16>,

    // This is the index of the key array in the builder at which an
    // overflow was detected. This will be set when the underlying builder
    // could not detect a-priori that inserting the value would cause an
    // an overflow e.g. b/c it didn't know about this parent builder's
    // max_cardinality.
    overflow_index: Option<usize>,
}

impl<T, T8, T16> AdaptiveDictionaryBuilder<T8, T16>
where
    T8: ArrayBuilderConstructor<Args = T>,
    T16: ArrayBuilderConstructor<Args = T>,
{
    pub fn new(options: &DictionaryOptions, constructor_args: T) -> Self {
        // choose the default dictionary index type to be the smallest that can
        // hold the min cardinality
        let variant = if options.min_cardinality <= u8::MAX.into() {
            DictIndexVariant::UInt8(T8::new(constructor_args))
        } else {
            DictIndexVariant::UInt16(T16::new(constructor_args))
        };

        Self {
            max_cardinality: options.max_cardinality,
            variant,
            overflow_index: None,
        }
    }
}

impl<T8, T16> AdaptiveDictionaryBuilder<T8, T16>
where
    T8: UpdateDictionaryIndexInto<T16>,
    T16: ArrayBuilderConstructor,
{
    fn upgrade_key(&mut self) -> Result<()> {
        match &mut self.variant {
            DictIndexVariant::UInt8(dict_builder) => {
                // if the max cardinality is less than what the next bigger
                // index type can hold, we don't want to upgrade
                if self.max_cardinality <= u8::MAX.into() {
                    return DictOverflowSnafu.fail();
                }

                let next_bigger = dict_builder.upgrade_into();
                self.variant = DictIndexVariant::UInt16(next_bigger);

                Ok(())
            }
            _ => DictOverflowSnafu.fail(),
        }
    }
}

impl<T, T8, T16> AdaptiveDictionaryBuilder<T8, T16>
where
    T8: DictionaryBuilder<UInt8Type> + ConvertToNativeHelper,
    <T8 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
    T16: DictionaryBuilder<UInt16Type> + ConvertToNativeHelper,
    <T16 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
{
    pub fn to_native<TN>(&mut self, builder: &mut TN)
    where
        TN: ArrayAppend<Native = T>,
    {
        // safety: we're OK to call expect() here because UncheckedArrayAdapter will never return Error
        // which means populate_native_builder will also not return an error
        self.to_native_checked(&mut UncheckedArrayBuilderAdapter { inner: builder })
            .expect("can convert to native");
    }

    pub fn to_native_checked<TN>(&mut self, builder: &mut TN) -> std::result::Result<(), ArrowError>
    where
        TN: CheckedArrayAppend<Native = T>,
    {
        match &mut self.variant {
            DictIndexVariant::UInt8(dict_builder) => {
                let result = dict_builder.finish();
                populate_native_builder::<_, UInt8Type, <T8 as ConvertToNativeHelper>::Accessor, _>(
                    &result,
                    builder,
                    self.overflow_index,
                )
            }

            DictIndexVariant::UInt16(dict_builder) => {
                let result = dict_builder.finish();
                populate_native_builder::<_, UInt16Type, <T16 as ConvertToNativeHelper>::Accessor, _>(
                    &result,
                    builder,
                    self.overflow_index,
                )
            }
        }
    }
}

/// TODO -- comment on what this is doing & why we have it
struct UncheckedArrayBuilderAdapter<'a, T>
where
    T: ArrayAppend,
{
    inner: &'a mut T,
}

impl<'a, T> CheckedArrayAppend for UncheckedArrayBuilderAdapter<'a, T>
where
    T: ArrayAppend,
{
    type Native = T::Native;

    fn append_value(&mut self, value: &Self::Native) -> std::result::Result<(), ArrowError> {
        self.inner.append_value(value);
        Ok(())
    }
}

// This helper function populates the native builder from the dict values in a way
// that is generic over the type of dictionary key
fn populate_native_builder<T, K, V, TN>(
    dict_arr: &DictionaryArray<K>,
    builder: &mut TN,
    overflow_index: Option<usize>,
    // TODO don't like how have to call this out as std::result::Result?
    // maybe could avoid aliasing the result in this package instead
) -> std::result::Result<(), ArrowError>
where
    TN: CheckedArrayAppend<Native = T>,
    K: ArrowDictionaryKeyType,
    <K as ArrowPrimitiveType>::Native: Into<usize>,
    V: NullableArrayAccessor<Native = T> + 'static,
{
    let keys = dict_arr.keys();
    // safety: in the places this method is called, the type constraints are enforced
    // in a way that this cast should be safe
    let values = dict_arr
        .values()
        .as_any()
        .downcast_ref::<V>()
        .expect("expect dictionary value types match native builder type");

    for i in 0..dict_arr.len() {
        if !keys.is_valid(i) {
            // TODO handle nulls in https://github.com/open-telemetry/otel-arrow/issues/534
            todo!("nulls not currently supported in adaptive array builders");
        }
        let key = keys.value(i);
        let index = key.into();

        // break if we find the index that caused the overflow
        if overflow_index == Some(index) {
            break;
        }

        // safety: we've already checked that the key at this index is valid
        let value = values
            .value_at(index)
            .expect("expect index in dict values array to be valid");
        builder.append_value(&value)?;
    }

    Ok(())
}

impl<T, T8, T16> AdaptiveDictionaryBuilder<T8, T16>
where
    T8: DictionaryArrayAppend<UInt8Type, Native = T> + UpdateDictionaryIndexInto<T16>,
    T16: DictionaryArrayAppend<UInt16Type, Native = T> + ArrayBuilderConstructor,
{
    pub fn append_value(&mut self, value: &T) -> Result<usize> {
        let append_result = match &mut self.variant {
            DictIndexVariant::UInt8(dict_builder) => dict_builder.append_value(value),
            DictIndexVariant::UInt16(dict_builder) => dict_builder.append_value(value),
        };

        match append_result {
            Ok(index) => {
                if index + 1 > self.max_cardinality as usize {
                    // if we're here, it means we did append successfully to the underlying builder
                    // but we shouldn't have, because have overflowed the configured max cardinality
                    self.overflow_index = Some(index);
                    Err(DictionaryBuilderError::DictOverflow {})
                } else {
                    Ok(index)
                }
            }
            Err(DictionaryBuilderError::DictOverflow {}) => {
                self.upgrade_key()?;
                self.append_value(value)
            }
        }
    }
}

impl<T, T8, T16> AdaptiveDictionaryBuilder<T8, T16>
where
    T8: CheckedDictionaryArrayAppend<UInt8Type, Native = T> + UpdateDictionaryIndexInto<T16>,
    T16: CheckedDictionaryArrayAppend<UInt16Type, Native = T> + ArrayBuilderConstructor,
{
    pub fn append_value_checked(&mut self, value: &T) -> checked::Result<usize> {
        let append_result = match &mut self.variant {
            DictIndexVariant::UInt8(dict_builder) => dict_builder.append_value(value),
            DictIndexVariant::UInt16(dict_builder) => dict_builder.append_value(value),
        };

        match append_result {
            Ok(index) => {
                if index + 1 > self.max_cardinality as usize {
                    // if we're here, it means we did append successfully to the underlying builder
                    // but we shouldn't have, because have overflowed the configured max cardinality
                    self.overflow_index = Some(index);
                    Err(checked::DictionaryBuilderError::DictOverflow {})
                } else {
                    Ok(index)
                }
            }
            Err(checked::DictionaryBuilderError::DictOverflow {}) => {
                self.upgrade_key().map_err(|err| match err {
                    DictionaryBuilderError::DictOverflow {} => {
                        checked::DictionaryBuilderError::DictOverflow {}
                    }
                })?;
                self.append_value_checked(value)
            }

            // return other types of errors to caller
            e => return e,
        }
    }
}

impl<T8, T16> AdaptiveDictionaryBuilder<T8, T16>
where
    T8: DictionaryBuilder<UInt8Type>,
    T16: DictionaryBuilder<UInt16Type>,
{
    pub fn finish(&mut self) -> super::ArrayWithType {
        match &mut self.variant {
            DictIndexVariant::UInt8(u8_dict_builder) => u8_dict_builder.finish().into(),
            DictIndexVariant::UInt16(u16_dict_builder) => u16_dict_builder.finish().into(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::sync::Arc;

    use arrow::array::{
        StringBuilder, StringDictionaryBuilder, UInt8Array, UInt8DictionaryArray, UInt16Array,
        UInt16DictionaryArray,
    };
    use arrow::datatypes::{DataType, UInt8Type, UInt16Type};

    type TestDictBuilder = AdaptiveDictionaryBuilder<
        StringDictionaryBuilder<UInt8Type>,
        StringDictionaryBuilder<UInt16Type>,
    >;

    #[test]
    fn test_dict_builder() {
        let mut dict_builder = TestDictBuilder::new(
            &DictionaryOptions {
                max_cardinality: u16::MAX,
                min_cardinality: u8::MAX.into(),
            },
            (),
        );

        let index = dict_builder.append_value(&"a".to_string()).unwrap();
        assert_eq!(index, 0);
        let index = dict_builder.append_value(&"a".to_string()).unwrap();
        assert_eq!(index, 0);
        let index = dict_builder.append_value(&"b".to_string()).unwrap();
        assert_eq!(index, 1);

        let result = dict_builder.finish();

        assert_eq!(
            result.data_type,
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8))
        );

        let mut expected_dict_values = StringBuilder::new();
        expected_dict_values.append_value("a");
        expected_dict_values.append_value("b");
        let expected_dict_keys = UInt8Array::from_iter_values(vec![0, 0, 1]);
        let expected =
            UInt8DictionaryArray::new(expected_dict_keys, Arc::new(expected_dict_values.finish()));

        assert_eq!(
            result
                .array
                .as_any()
                .downcast_ref::<UInt8DictionaryArray>()
                .unwrap(),
            &expected
        );
    }

    #[test]
    fn test_dict_builder_update_index_type() {
        let mut dict_builder = TestDictBuilder::new(
            &DictionaryOptions {
                max_cardinality: u16::MAX,
                min_cardinality: u8::MAX.into(),
            },
            Default::default(),
        );

        for i in 0..257 {
            let _ = dict_builder.append_value(&i.to_string()).unwrap();
        }

        let result = dict_builder.finish();

        assert_eq!(
            result.data_type,
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
        );

        // check that the dictionary is the correct type
        let dict_array = result
            .array
            .as_any()
            .downcast_ref::<UInt16DictionaryArray>();
        assert!(dict_array.is_some(), "Expected a UInt16DictionaryArray");
    }

    #[test]
    fn test_dict_max_cardinality() {
        let mut dict_builder = TestDictBuilder::new(
            &DictionaryOptions {
                max_cardinality: u8::MAX as u16 + 1,
                min_cardinality: u8::MAX as u16 + 1,
            },
            Default::default(),
        );

        for i in 0..u8::MAX {
            let _ = dict_builder.append_value(&i.to_string()).unwrap();
        }

        // this should be fine
        let _ = dict_builder.append_value(&"a".to_string()).unwrap();

        // should overflow the max cardinality
        let result = dict_builder.append_value(&"b".to_string());
        assert!(
            result.is_err(),
            "Expected an error due to exceeding max cardinality"
        );
        assert!(
            matches!(result.unwrap_err(), DictionaryBuilderError::DictOverflow {}),
            "Expected a DictOverflow error"
        );
    }

    #[test]
    fn test_dict_min_cardinality() {
        // test that we can force the dictionary index to be bigger type than is needed
        // by specifying the min cardinality.
        let mut dict_builder = TestDictBuilder::new(
            &DictionaryOptions {
                max_cardinality: u16::MAX,
                min_cardinality: u16::MAX,
            },
            Default::default(),
        );

        let _ = dict_builder.append_value(&"a".to_string()).unwrap();
        let _ = dict_builder.append_value(&"a".to_string()).unwrap();
        let _ = dict_builder.append_value(&"b".to_string()).unwrap();

        let result = dict_builder.finish();

        assert_eq!(
            result.data_type,
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
        );

        let mut expected_dict_values = StringBuilder::new();
        expected_dict_values.append_value("a");
        expected_dict_values.append_value("b");
        let expected_dict_keys = UInt16Array::from_iter_values(vec![0, 0, 1]);
        let expected =
            UInt16DictionaryArray::new(expected_dict_keys, Arc::new(expected_dict_values.finish()));

        assert_eq!(
            result
                .array
                .as_any()
                .downcast_ref::<UInt16DictionaryArray>()
                .unwrap(),
            &expected
        );
    }

    #[test]
    fn test_dict_arbitrary_max_cardinality() {
        // check that we support a max-cardinality that is arbitrarily aligned
        // e.g. not necessarily alighed to u8/u16 max values
        let mut dict_builder = TestDictBuilder::new(
            &DictionaryOptions {
                max_cardinality: 4,
                min_cardinality: 4,
            },
            Default::default(),
        );

        let _ = dict_builder.append_value(&"a".to_string()).unwrap();
        let _ = dict_builder.append_value(&"b".to_string()).unwrap();
        let _ = dict_builder.append_value(&"c".to_string()).unwrap();
        let _ = dict_builder.append_value(&"d".to_string()).unwrap();

        // this should be OK, we are re-adding an existing value so it should not
        // affect the size of the dictionary
        let _ = dict_builder.append_value(&"d".to_string()).unwrap();

        // this should exceed the max cardinality:
        let result = dict_builder.append_value(&"e".to_string());

        assert!(
            matches!(result.unwrap_err(), DictionaryBuilderError::DictOverflow {}),
            "Expected a DictOverflow error"
        );
    }
}
