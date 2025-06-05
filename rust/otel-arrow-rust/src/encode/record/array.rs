// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! This module provides adaptive array builders for Arrow arrays.
//!
//! Often for OTel-Arrow, we have columns that are optional on the schema. For example, the Boolean
//! column may not be present in a record batch representing a list of attributes only be present
//! if there are some boolean type values in the list.
//!
//! There are also cases where we want to dynamically use dictionary encoding with the smallest index
//! the cardinality of the allows.
//!
//! This module contains adaptive array builders that can dynamically create either no array (for
//! an all-null) column, an array that may be a dictionary, of an array or native types. It will
//! handle converting between different builders dynamically  based on the data which is appended.

use arrow::array::{
    ArrayRef, ArrowPrimitiveType, BinaryBuilder, BinaryDictionaryBuilder, PrimitiveBuilder,
    PrimitiveDictionaryBuilder, StringBuilder, StringDictionaryBuilder,
};
use arrow::datatypes::{
    ArrowDictionaryKeyType, DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::error::ArrowError;

use crate::arrays::NullableArrayAccessor;
use crate::encode::record::array::dictionary::DictionaryBuilder;

use dictionary::{
    AdaptiveDictionaryBuilder, CheckedDictionaryArrayAppend, ConvertToNativeHelper,
    DictionaryArrayAppend, DictionaryBuilderError, DictionaryOptions, UpdateDictionaryIndexInto,
    checked,
};

pub mod binary;
pub mod boolean;
pub mod dictionary;
pub mod fixed_size_binary;
pub mod primitive;
pub mod string;

/// This is the base trait that array builders should implement.
pub trait ArrayBuilder {
    fn finish(&mut self) -> ArrayWithType;
}

pub struct ArrayWithType {
    pub array: ArrayRef,
    pub data_type: DataType,
}

/// This is a helper trait that allows the adaptive builders to construct new
/// instances of the builder dynamically
pub trait ArrayBuilderConstructor {
    type Args; // TODO, should be clone?

    fn new(args: Self::Args) -> Self;

    // TODO, at some point we may consider optionally adding a
    // with_capacity function here that could be used to create
    // a builder with pre-allocated buffers
}

pub trait ArrayAppend {
    type Native;

    fn append_value(&mut self, value: &Self::Native);
}

/// Some underlying builders may have `append_` methods that return results. For example,
/// FixedSizeBinary's builders can return an error if a byte array of the wrong length is passed.
///
/// In this case, underlying builders should implement this trait and callers can use it when
/// there's uncertainty that the value they've passed is valid.
pub trait CheckedArrayAppend {
    type Native;

    fn append_value(&mut self, value: &Self::Native) -> Result<(), ArrowError>;
}

/// This enum is a container that abstracts array builder which is either
/// dictionary or native. It converts from the dictionary builder to the
/// native builder when the dictionary builder overflows.
enum MaybeDictionaryBuilder<NativeBuilder, DictBuilderU8, DictBuilderU16> {
    Native(NativeBuilder),
    Dictionary(AdaptiveDictionaryBuilder<DictBuilderU8, DictBuilderU16>),
}

// impl<T, TN, TD8, TD16> ArrayAppend for MaybeDictionaryBuilder<TN, TD8, TD16>
// where
//     TN: ArrayAppend<Native = T> + ArrayBuilderConstructor,
//     TD8: DictionaryArrayBuilder<UInt8Type, Native = T>
//         + ArrayBuilderConstructor
//         + ConvertToNativeHelper,
//     <TD8 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
//     TD16: DictionaryArrayBuilder<UInt16Type, Native = T>
//         + ArrayBuilderConstructor
//         + ConvertToNativeHelper,
//     <TD16 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
//     TD8: UpdateDictionaryIndexInto<TD16>,
// {
//     type Native = T;

//     fn append_value(
//         &mut self,
//         value: &<MaybeDictionaryBuilder<TN, TD8, TD16> as ArrayAppend>::Native,
//     ) {
//         match self {
//             Self::Native(array_builder) => array_builder.append_value(value),
//             Self::Dictionary(dict_array_builder) => match dict_array_builder.append_value(value) {
//                 // we've overflowed the dictionary, so we must convert to the native builder type
//                 Err(DictionaryBuilderError::DictOverflow {}) => {
//                     let mut native = TN::new();
//                     dict_array_builder.to_native(&mut native);
//                     native.append_value(value);
//                     *self = Self::Native(native);
//                 }
//                 _ => {
//                     // do nothing here, as the append was successful
//                 }
//             },
//         }
//     }
// }

// impl<T, TN, TD8, TD16> CheckedArrayAppend for MaybeDictionaryBuilder<TN, TD8, TD16>
// where
//     TN: CheckedArrayAppend<Native = T> + ArrayBuilderConstructor,
//     TD8: DictionaryArrayBuilder<UInt8Type, Native = T>
//         + ArrayBuilderConstructor
//         + ConvertToNativeHelper,
//     <TD8 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
//     TD16: DictionaryArrayBuilder<UInt16Type, Native = T>
//         + ArrayBuilderConstructor
//         + ConvertToNativeHelper,
//     <TD16 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
//     TD8: UpdateDictionaryIndexInto<TD16>,
// {
//     type Native = T;

//     fn append_value_checked(
//         &mut self,
//         value: &<MaybeDictionaryBuilder<TN, TD8, TD16> as CheckedArrayAppend>::Native,
//     ) -> Result<(), ArrowError> {
//         match self {
//             Self::Native(array_builder) => array_builder.append_value_checked(value),
//             Self::Dictionary(dict_array_builder) => match dict_array_builder.append_value(value) {
//                 // we've overflowed the dictionary, so we must convert to the native builder type
//                 Err(DictionaryBuilderError::DictOverflow {}) => {
//                     let mut native = TN::new();
//                     dict_array_builder.to_native_checked(&mut native);
//                     *self = Self::Native(native);
//                     self.append_value_checked(value)
//                 }
//                 _ => Ok(()),
//             },
//         }
//     }
// }

impl<TN, TD8, TD16> ArrayBuilder for MaybeDictionaryBuilder<TN, TD8, TD16>
where
    TN: ArrayBuilder,
    TD8: DictionaryBuilder<UInt8Type>,
    TD16: DictionaryBuilder<UInt16Type>,
{
    fn finish(&mut self) -> ArrayWithType {
        match self {
            Self::Dictionary(dict_array_builder) => dict_array_builder.finish(),
            Self::Native(array_builder) => array_builder.finish(),
        }
    }
}

#[derive(Default)]
pub struct ArrayOptions {
    pub dictionary_options: Option<DictionaryOptions>,
    pub nullable: bool,
}

pub struct AdaptiveArrayBuilder<TArgs, TN, TD8, TD16> {
    dictionary_options: Option<DictionaryOptions>,
    inner: Option<MaybeDictionaryBuilder<TN, TD8, TD16>>,
    inner_args: TArgs,
}

impl<TN, TD8, TD16> AdaptiveArrayBuilder<NoArgs, TN, TD8, TD16>
where
    TN: ArrayBuilderConstructor<Args = NoArgs>,
    TD8: ArrayBuilderConstructor<Args = NoArgs>,
    TD16: ArrayBuilderConstructor<Args = NoArgs>,
{
    pub fn new(options: ArrayOptions) -> Self {
        Self::new_with_args(options, ())
    }
}

impl<TArgs, TN, TD8, TD16> AdaptiveArrayBuilder<TArgs, TN, TD8, TD16>
where
    TArgs: Clone,
    TN: ArrayBuilderConstructor<Args = TArgs>,
    TD8: ArrayBuilderConstructor<Args = TArgs>,
    TD16: ArrayBuilderConstructor<Args = TArgs>,
{
    pub fn new_with_args(options: ArrayOptions, args: TArgs) -> Self {
        let inner = if options.nullable {
            None
        } else {
            Some(Self::initial_builder(
                args.clone(),
                &options.dictionary_options,
            ))
        };

        Self {
            dictionary_options: options.dictionary_options,
            inner,
            inner_args: args,
        }
    }

    // Initializes the builder, which may either be a builder for the, if dictionary
    // options is `Some`, otherwise it will construct the native builder builder variant
    fn initial_builder(
        args: TArgs,
        dictionary_options: &Option<DictionaryOptions>,
    ) -> MaybeDictionaryBuilder<TN, TD8, TD16> {
        match dictionary_options.as_ref() {
            Some(dictionary_options) => MaybeDictionaryBuilder::Dictionary(
                AdaptiveDictionaryBuilder::new(dictionary_options, args),
            ),
            None => MaybeDictionaryBuilder::Native(TN::new(args)),
        }
    }

    fn initialize_inner(&mut self) {
        if self.inner.is_none() {
            // TODO -- when we handle nulls here we need to keep track of how many
            // nulls have been appended before the first value, and prefix this
            // newly initialized array with that number of nulls
            // https://github.com/open-telemetry/otel-arrow/issues/534
            self.inner = Some(Self::initial_builder(
                self.inner_args.clone(),
                &self.dictionary_options,
            ));
        }
    }
}

impl<T, TArgs, TN, TD8, TD16> ArrayAppend for AdaptiveArrayBuilder<TArgs, TN, TD8, TD16>
where
    TArgs: Clone,
    TN: ArrayAppend<Native = T> + ArrayBuilderConstructor<Args = TArgs>,
    TD8: DictionaryArrayAppend<UInt8Type, Native = T>
        + DictionaryBuilder<UInt8Type>
        + ArrayBuilderConstructor<Args = TArgs>
        + ConvertToNativeHelper
        + UpdateDictionaryIndexInto<TD16>,
    <TD8 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
    TD16: DictionaryArrayAppend<UInt16Type, Native = T>
        + DictionaryBuilder<UInt16Type>
        + ArrayBuilderConstructor<Args = TArgs>
        + ConvertToNativeHelper,
    <TD16 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
{
    type Native = T;

    fn append_value(&mut self, value: &T) {
        self.initialize_inner();
        let inner = self
            .inner
            .as_mut()
            .expect("inner should now be initialized");
        match inner {
            MaybeDictionaryBuilder::Native(native_builder) => {
                native_builder.append_value(value);
            }
            MaybeDictionaryBuilder::Dictionary(dictionary_builder) => {
                match dictionary_builder.append_value(value) {
                    Ok(_) => {}
                    Err(DictionaryBuilderError::DictOverflow {}) => {
                        let mut native = TN::new(self.inner_args.clone());
                        dictionary_builder.to_native(&mut native);
                        self.inner = Some(MaybeDictionaryBuilder::Native(native));
                        self.append_value(value);
                    }
                }
            }
        }
    }
}

impl<T, TArgs, TN, TD8, TD16> AdaptiveArrayBuilder<TArgs, TN, TD8, TD16>
where
    TArgs: Clone,
    TN: CheckedArrayAppend<Native = T> + ArrayBuilderConstructor<Args = TArgs>,
    TD8: CheckedDictionaryArrayAppend<UInt8Type, Native = T>
        + DictionaryBuilder<UInt8Type>
        + ArrayBuilderConstructor<Args = TArgs>
        + ConvertToNativeHelper
        + UpdateDictionaryIndexInto<TD16>,
    <TD8 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
    TD16: CheckedDictionaryArrayAppend<UInt16Type, Native = T>
        + DictionaryBuilder<UInt16Type>
        + ArrayBuilderConstructor<Args = TArgs>
        + ConvertToNativeHelper,
    <TD16 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
{
    fn append_value_checked(&mut self, value: &T) -> Result<(), ArrowError> {
        self.initialize_inner();
        let inner = self
            .inner
            .as_mut()
            .expect("inner should now be initialized");
        match inner {
            MaybeDictionaryBuilder::Native(native_builder) => native_builder.append_value(value),
            MaybeDictionaryBuilder::Dictionary(dictionary_builder) => {
                match dictionary_builder.append_value_checked(value) {
                    Ok(_) => {
                        // append succeeded
                        Ok(())
                    }

                    Err(checked::DictionaryBuilderError::DictOverflow {}) => {
                        let mut native = TN::new(self.inner_args.clone());
                        dictionary_builder.to_native_checked(&mut native)?;
                        self.inner = Some(MaybeDictionaryBuilder::Native(native));
                        self.append_value_checked(value)
                    }
                    Err(checked::DictionaryBuilderError::CheckedBuilderError {
                        source: arrow_error,
                    }) => Err(arrow_error),
                }
            }
        }
    }
}

impl<TArgs, TN, TD8, TD16> AdaptiveArrayBuilder<TArgs, TN, TD8, TD16>
where
    TN: ArrayBuilder,
    TD8: DictionaryBuilder<UInt8Type>,
    TD16: DictionaryBuilder<UInt16Type>,
{
    fn finish(&mut self) -> Option<ArrayWithType> {
        self.inner.as_mut().map(|builder| builder.finish())
    }
}

// arg type for noargs constructor
type NoArgs = ();

pub type StringArrayBuilder = AdaptiveArrayBuilder<
    NoArgs,
    StringBuilder,
    StringDictionaryBuilder<UInt8Type>,
    StringDictionaryBuilder<UInt16Type>,
>;

pub type BinaryArrayBuilder = AdaptiveArrayBuilder<
    NoArgs,
    BinaryBuilder,
    BinaryDictionaryBuilder<UInt8Type>,
    BinaryDictionaryBuilder<UInt16Type>,
>;

pub type PrimitiveArrayBuilder<T> = AdaptiveArrayBuilder<
    NoArgs,
    PrimitiveBuilder<T>,
    PrimitiveDictionaryBuilder<UInt8Type, T>,
    PrimitiveDictionaryBuilder<UInt16Type, T>,
>;

// aliases for adaptive primitive array builders
pub type Float32ArrayBuilder = PrimitiveArrayBuilder<Float32Type>;
pub type Float64ArrayBuilder = PrimitiveArrayBuilder<Float64Type>;
pub type UInt8ArrayBuilder = PrimitiveArrayBuilder<UInt8Type>;
pub type UInt16ArrayBuilder = PrimitiveArrayBuilder<UInt16Type>;
pub type UInt32ArrayBuilder = PrimitiveArrayBuilder<UInt32Type>;
pub type UInt64ArrayBuilder = PrimitiveArrayBuilder<UInt64Type>;
pub type Int8ArrayBuilder = PrimitiveArrayBuilder<Int8Type>;
pub type Int16ArrayBuilder = PrimitiveArrayBuilder<Int16Type>;
pub type Int32ArrayBuilder = PrimitiveArrayBuilder<Int32Type>;
pub type Int64ArrayBuilder = PrimitiveArrayBuilder<Int64Type>;

#[cfg(test)]
pub mod test {
    use super::*;

    use std::sync::Arc;

    use arrow::array::{DictionaryArray, StringArray, UInt8Array, UInt8DictionaryArray};
    use arrow::datatypes::DataType;

    fn test_array_builder_generic<T, TN, TD8, TD16>(
        array_builder_factory: impl FnOnce(ArrayOptions) -> AdaptiveArrayBuilder<NoArgs, TN, TD8, TD16>,
        values: Vec<T>,
        expected_data_type: DataType,
    ) where
        T: PartialEq + std::fmt::Debug,
        TN: ArrayAppend<Native = T> + ArrayBuilderConstructor<Args = NoArgs> + ArrayBuilder,
        TD8: DictionaryArrayAppend<UInt8Type, Native = T>
            + DictionaryBuilder<UInt8Type>
            + ArrayBuilderConstructor<Args = NoArgs>
            + ConvertToNativeHelper
            + UpdateDictionaryIndexInto<TD16>,
        <TD8 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
        TD16: DictionaryArrayAppend<UInt16Type, Native = T>
            + DictionaryBuilder<UInt16Type>
            + ArrayBuilderConstructor<Args = NoArgs>
            + ConvertToNativeHelper,
        <TD16 as ConvertToNativeHelper>::Accessor: NullableArrayAccessor<Native = T> + 'static,
    {
        let mut builder = array_builder_factory(ArrayOptions {
            nullable: true,
            dictionary_options: Some(DictionaryOptions {
                max_cardinality: 4,
                min_cardinality: 4,
            }),
        });

        // expect that for empty array, we get a None value because the builder is nullable
        let result = builder.finish();
        assert!(result.is_none());

        // expect that when we add values, we get a dictionary
        builder.append_value(&values[0]);
        builder.append_value(&values[0]);
        builder.append_value(&values[1]);

        let result = builder.finish().unwrap();
        assert_eq!(
            result.data_type,
            DataType::Dictionary(
                Box::new(DataType::UInt8),
                Box::new(expected_data_type.clone())
            )
        );

        // TODO check the dict keys here

        assert_eq!(result.array.len(), 3);

        let dict_array = result
            .array
            .as_any()
            .downcast_ref::<DictionaryArray<UInt8Type>>()
            .unwrap();
        let dict_keys = dict_array.keys();
        assert_eq!(dict_keys, &UInt8Array::from_iter_values(vec![0, 0, 1]));
        let dict_values = dict_array
            .values()
            .as_any()
            .downcast_ref::<<TD8 as ConvertToNativeHelper>::Accessor>()
            .unwrap();
        assert_eq!(dict_values.value_at(0).unwrap(), values[0]);
        assert_eq!(dict_values.value_at(1).unwrap(), values[1]);
    }

    #[test]
    fn test_array_builder() {
        test_array_builder_generic(
            |opts| UInt8ArrayBuilder::new(opts),
            vec![0, 1],
            DataType::UInt8,
        );
        test_array_builder_generic(
            |opts| UInt16ArrayBuilder::new(opts),
            vec![0, 1],
            DataType::UInt16,
        );
        test_array_builder_generic(
            |opts| UInt32ArrayBuilder::new(opts),
            vec![0, 1],
            DataType::UInt32,
        );
        test_array_builder_generic(
            |opts| UInt64ArrayBuilder::new(opts),
            vec![0, 1],
            DataType::UInt64,
        );
        test_array_builder_generic(
            |opts| Int8ArrayBuilder::new(opts),
            vec![0, 1],
            DataType::Int8,
        );
        test_array_builder_generic(
            |opts| Int16ArrayBuilder::new(opts),
            vec![0, 1],
            DataType::Int16,
        );
        test_array_builder_generic(
            |opts| Int32ArrayBuilder::new(opts),
            vec![0, 1],
            DataType::Int32,
        );
        test_array_builder_generic(
            |opts| Int64ArrayBuilder::new(opts),
            vec![0, 1],
            DataType::Int64,
        );
        test_array_builder_generic(
            |opts| Float32ArrayBuilder::new(opts),
            vec![0.0, 1.0],
            DataType::Float32,
        );
        test_array_builder_generic(
            |opts| Float64ArrayBuilder::new(opts),
            vec![0.0, 1.1],
            DataType::Float64,
        );
        test_array_builder_generic(
            |opts| StringArrayBuilder::new(opts),
            vec!["a".to_string(), "b".to_string()],
            DataType::Utf8,
        );
        test_array_builder_generic(
            |opts| BinaryArrayBuilder::new(opts),
            vec![b"a".to_vec(), b"b".to_vec()],
            DataType::Binary,
        );
    }

    #[test]
    fn test_array_builder_TODO_delete_this() {
        let mut builder = StringArrayBuilder::new(ArrayOptions {
            nullable: true,
            dictionary_options: Some(DictionaryOptions {
                min_cardinality: 4,
                max_cardinality: 4,
            }),
        });

        // expect that for empty array, we get a None value because the builder is nullable
        let result = builder.finish();
        assert!(result.is_none());

        // expect that when we add values, we get a dictionary
        builder.append_value(&"a".to_string());
        builder.append_value(&"a".to_string());
        builder.append_value(&"b".to_string());

        let result = builder.finish().unwrap();
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
    fn test_array_builder_non_nullable_empty() {
        let mut builder = StringArrayBuilder::new(ArrayOptions {
            nullable: false,
            dictionary_options: Some(DictionaryOptions {
                min_cardinality: 4,
                max_cardinality: 4,
            }),
        });

        // check that since the type we're building is not nullable, we get an empty array
        let result = builder.finish().unwrap();
        assert_eq!(
            result.data_type,
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8))
        );
        assert_eq!(result.array.len(), 0);
    }

    #[test]
    fn test_array_builder_dict_overflow() {
        let mut builder = StringArrayBuilder::new(ArrayOptions {
            nullable: false,
            dictionary_options: Some(DictionaryOptions {
                min_cardinality: 4,
                max_cardinality: 4,
            }),
        });

        // expect that when we add values, we get a dictionary
        builder.append_value(&"a".to_string());
        builder.append_value(&"b".to_string());
        builder.append_value(&"c".to_string());
        builder.append_value(&"d".to_string());
        builder.append_value(&"e".to_string());

        let result = builder.finish().unwrap();
        assert_eq!(result.data_type, DataType::Utf8);

        let mut expected_values = StringBuilder::new();
        expected_values.append_value("a");
        expected_values.append_value("b");
        expected_values.append_value("c");
        expected_values.append_value("d");
        expected_values.append_value("e");

        assert_eq!(
            result.array.as_any().downcast_ref::<StringArray>().unwrap(),
            &expected_values.finish()
        )
    }
}
