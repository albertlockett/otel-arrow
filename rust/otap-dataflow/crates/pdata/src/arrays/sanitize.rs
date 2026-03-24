// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Utilities for sanitizing arrow arrays.
//!
//! This procedure involves removing any unreferenced data in any arrow array, specifically:
//! - removing any dictionary values that have no keys pointing to them

use std::borrow::Cow;
use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, DictionaryArray, MutableArrayData, PrimitiveArray,
        RecordBatch, StringArray, make_array,
    },
    buffer::ScalarBuffer,
    datatypes::{ArrowDictionaryKeyType, ArrowNativeType, UInt8Type, UInt16Type},
};
use arrow_schema::DataType;

use crate::otap::filter::BitmapPage;

/// sanitize all columns in the
pub fn sanitize_record_batch(record_batch: &RecordBatch) -> Option<RecordBatch> {
    let mut columns = Cow::from(record_batch.columns());

    for i in 0..record_batch.num_columns() {
        let column = record_batch.column(i);
        match column.data_type() {
            DataType::Dictionary(k, _) => match k.as_ref() {
                DataType::UInt8 => {
                    let dict_arr = column
                        .as_any()
                        .downcast_ref::<DictionaryArray<UInt8Type>>()
                        .expect("can downcast to dict");
                    if let Some(sanitized) = sanitized_dict(dict_arr) {
                        columns.to_mut()[i] = Arc::new(sanitized)
                    }
                }
                DataType::UInt16 => {
                    let dict_arr = column
                        .as_any()
                        .downcast_ref::<DictionaryArray<UInt16Type>>()
                        .expect("can downcast to dict");
                    if let Some(sanitized) = sanitized_dict(dict_arr) {
                        columns.to_mut()[i] = Arc::new(sanitized)
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }

    match columns {
        // no modifications were made:
        Cow::Borrowed(_) => None,

        // create new batch with sanitized columns
        Cow::Owned(new_columns) => {
            // safety: we haven't changed the length/type of any column, nor the column order
            // so it is safe to expect that try_new will not error here
            let sanitized_batch = RecordBatch::try_new(record_batch.schema(), new_columns)
                .expect("can create sanitized batch");
            Some(sanitized_batch)
        }
    }
}

/// Ensures that the values inside a dictionary with no keys pointing to them are removed.
fn sanitized_dict<K: ArrowDictionaryKeyType>(
    dict_arr: &DictionaryArray<K>,
) -> Option<DictionaryArray<K>>
where
    K::Native: SanitizeDictHelper,
{
    // first determine which keys actually point to dictionary values
    let mut live_values_set = BitmapPage::new();
    let mut live_values_count = 0;
    let mut live_values_total_bytes = 0;
    let dict_values = dict_arr.values();
    let dict_keys = dict_arr.keys();
    for key in dict_keys {
        if let Some(key) = key {
            let key = key.as_u16();
            if !live_values_set.contains(key) {
                live_values_set.insert(key);
                live_values_count += 1;
                live_values_total_bytes += dict_vals_byte_length(dict_values, key as usize);
                if live_values_count >= dict_values.len() {
                    // all the values are active, just return the original dict array
                    return None;
                }
            }
        }
    }

    // build new values, and keep track of key remappings
    let dict_val_data = dict_values.to_data();
    let mut mutable_dict_val_data =
        MutableArrayData::new(vec![&dict_val_data], false, live_values_total_bytes);
    let mut remapped_keys = vec![0; dict_values.len()];
    let mut rows_removed = 0;
    let mut last_valid_range_end = 0;
    let valid_ranges_iter = live_values_set.valid_slices_iter(dict_values.len());
    for (start, end) in valid_ranges_iter {
        if last_valid_range_end < start {
            rows_removed += start - last_valid_range_end;
        }

        mutable_dict_val_data.extend(0, start, end);
        for i in start..end {
            remapped_keys[i] = i - rows_removed
        }
        last_valid_range_end = end;
    }

    // build remapped dictionary keys
    let mut new_keys: Vec<K::Native> = vec![K::Native::default(); dict_keys.len()];
    for (i, dict_key) in dict_keys.iter().enumerate() {
        if let Some(key) = dict_key {
            let new_key = remapped_keys[key.as_usize()];
            new_keys[i] = <K::Native as SanitizeDictHelper>::from_usize(new_key);
        }
    }

    let new_keys_values = ScalarBuffer::from(new_keys);
    let new_keys = PrimitiveArray::<K>::new(new_keys_values, dict_keys.nulls().cloned());
    let new_values = make_array(mutable_dict_val_data.freeze());

    Some(DictionaryArray::new(new_keys, new_values))
}

fn dict_vals_byte_length(arr: &ArrayRef, index: usize) -> usize {
    match arr.data_type() {
        DataType::UInt8 | DataType::Int8 => 1,
        DataType::UInt16 | DataType::Int16 => 2,
        DataType::UInt32 | DataType::Int32 | DataType::Float32 => 4,
        DataType::UInt64
        | DataType::Int64
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => 8,
        DataType::FixedSizeBinary(len) => *len as usize,
        DataType::Utf8 => {
            let byte_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("can downcast to string arr");
            byte_arr.value_length(index) as usize
        }
        DataType::Binary => {
            let byte_arr = arr
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("can downcast to binary arr");
            byte_arr.value_length(index) as usize
        }

        // other types aren't used in OTAP for dictionary types, but we return zero
        // here just to be defensive in case someone calls this with an invalid batch
        _ => 0,
    }
}

// helper trait for making sanitize_dict generic over supported key types
trait SanitizeDictHelper {
    fn as_u16(&self) -> u16;

    fn from_usize(val: usize) -> Self;
}

impl SanitizeDictHelper for u8 {
    fn as_u16(&self) -> u16 {
        *self as u16
    }

    fn from_usize(val: usize) -> Self {
        val as Self
    }
}

impl SanitizeDictHelper for u16 {
    fn as_u16(&self) -> u16 {
        *self
    }

    fn from_usize(val: usize) -> Self {
        val as Self
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{DictionaryArray, StringArray, UInt16Array};

    #[test]
    fn test_sanitize_dict() {
        let input = DictionaryArray::new(
            UInt16Array::from_iter_values([1, 2, 5, 5, 6]),
            Arc::new(StringArray::from_iter_values([
                "0", "1", "2", "3", "4", "5", "6", "7",
            ])),
        );

        let result = sanitized_dict(&input);

        let expected = DictionaryArray::new(
            UInt16Array::from_iter_values([0, 1, 2, 2, 3]),
            Arc::new(StringArray::from_iter_values(["1", "2", "5", "6"])),
        );

        assert_eq!(result.unwrap(), expected)
    }

    #[test]
    fn test_sanitize_dict_all_keys_active() {
        let input = DictionaryArray::new(
            UInt16Array::from_iter_values([0, 1, 0, 1, 2, 1]),
            Arc::new(StringArray::from_iter_values(["0", "1", "2"])),
        );

        assert!(sanitized_dict(&input).is_none());
    }
}
