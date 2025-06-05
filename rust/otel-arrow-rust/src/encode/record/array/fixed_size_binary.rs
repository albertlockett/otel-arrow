// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, DictionaryArray, FixedSizeBinaryBuilder,
        FixedSizeBinaryDictionaryBuilder,
    },
    datatypes::{ArrowDictionaryKeyType, DataType},
    error::ArrowError,
};

use crate::encode::record::array::dictionary::{
    CheckedDictionaryArrayAppend, DictionaryArrayAppend, DictionaryBuilder,
};

use super::{ArrayBuilder, ArrayBuilderConstructor, ArrayWithType, CheckedArrayAppend};

impl CheckedArrayAppend for FixedSizeBinaryBuilder {
    type Native = Vec<u8>;

    fn append_value(&mut self, value: &Self::Native) -> Result<(), ArrowError> {
        self.append_value(value)
    }
}

impl ArrayBuilder for FixedSizeBinaryBuilder {
    fn finish(&mut self) -> ArrayWithType {
        let result = self.finish();
        let data_type = result.data_type().clone();
        ArrayWithType {
            array: Arc::new(result),
            data_type,
        }
    }
}

impl<K> CheckedDictionaryArrayAppend<K> for FixedSizeBinaryDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
    <K as ArrowPrimitiveType>::Native: Into<usize>,
{
    type Native = Vec<u8>;

    fn append_value(&mut self, value: &Self::Native) -> super::dictionary::checked::Result<usize> {
        match self.append(value) {
            Ok(index) => Ok(index.into()),
            Err(ArrowError::DictionaryKeyOverflowError) => {
                Err(super::dictionary::checked::DictionaryBuilderError::DictOverflow {})
            }
            Err(e) => Err(
                super::dictionary::checked::DictionaryBuilderError::CheckedBuilderError {
                    source: e,
                },
            ),
        }
    }
}

impl<K> DictionaryBuilder<K> for FixedSizeBinaryDictionaryBuilder<K>
where
    K: ArrowDictionaryKeyType,
{
    fn finish(&mut self) -> DictionaryArray<K> {
        self.finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, FixedSizeBinaryBuilder};
    use arrow::datatypes::DataType;

    #[test]
    fn test_fsb_builder() {
        // let mut fsb_builder = FixedSizeBinaryBuilder::new(4);
        // ArrayBuilder::append_value(&mut fsb_builder, &b"1234".to_vec());
        // ArrayBuilder::append_value(&mut fsb_builder, &b"5678".to_vec());
        // ArrayBuilder::append_value(&mut fsb_builder, &b"9012".to_vec());
        // let result = ArrayBuilder::finish(&mut fsb_builder);
        // assert_eq!(result.data_type, DataType::FixedSizeBinary(4));

        // let expected = FixedSizeBinaryArray::try_from_iter(
        //     vec![b"1234".to_vec(), b"5678".to_vec(), b"9012".to_vec()].iter(),
        // )
        // .unwrap();

        // assert_eq!(
        //     result
        //         .array
        //         .as_any()
        //         .downcast_ref::<FixedSizeBinaryArray>()
        //         .unwrap(),
        //     &expected
        // );
    }
}
