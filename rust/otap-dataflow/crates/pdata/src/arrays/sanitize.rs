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
        Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, MutableArrayData,
        PrimitiveArray, RecordBatch, StringArray, make_array,
    },
    buffer::ScalarBuffer,
    compute::kernels::filter::filter,
    datatypes::{ArrowDictionaryKeyType, ArrowNativeType, UInt8Type, UInt16Type},
    util::{bit_iterator::BitSliceIterator, bit_util},
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
    let dict_values = dict_arr.values();
    let dict_values_len = dict_values.len();
    let dict_keys = dict_arr.keys();

    // first determine which dictionary values are live (e.g. those with keys that reference them)
    let mut live_values_set = vec![0u8; bit_util::ceil(dict_values_len, 8)];
    let mut live_values_count = 0;
    let mut live_values_total_bytes = 0;

    // helper closure to set a value in the live values set. It also returns `true` if this
    // function can return early because it has determined that all dict values are live
    let mut set_value_live = |i: usize| {
        if !bit_util::get_bit(&live_values_set, i) {
            bit_util::set_bit(&mut live_values_set, i);
            live_values_count += 1;
            if live_values_count >= dict_values_len {
                return true;
            }
        }
        false
    };

    // go through the valid (non-null) ranges from the dictionary keys, marking which values are
    // live while trying to return early if all are live ...
    if let Some(nulls) = dict_keys.nulls() {
        for (start, end) in nulls.valid_slices() {
            for i in start..end {
                let all_values_live = set_value_live(i);
                if all_values_live {
                    return None;
                }
            }
        }
    } else {
        for i in 0..dict_keys.len() {
            let all_values_live = set_value_live(i);
            if all_values_live {
                return None;
            }
        }
    }

    // build dictionary key remapping
    let mut remapped_keys = vec![0; dict_values.len()];
    let mut rows_removed = 0;
    let mut last_valid_range_end = 0;
    for (start, end) in BitSliceIterator::new(&live_values_set, 0, dict_values_len) {
        if last_valid_range_end < start {
            rows_removed += start - last_valid_range_end;
        }

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

    // take only the live values
    let new_values = filter(
        dict_values,
        &BooleanArray::new_from_packed(live_values_set, 0, dict_values_len),
    )
    .expect("TODO why can we expect?");

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
