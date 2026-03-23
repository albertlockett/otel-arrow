// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Utilities for sanitizing arrow arrays.

use arrow::{
    array::{
        Array, ArrayData, ArrayRef, DictionaryArray, Int64Array, MutableArrayData, PrimitiveArray,
        StringArray, UInt8Array, make_array,
    },
    datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType},
};

use crate::otap::filter::BitmapPage;

/// Ensures that the values inside a dictionary with no keys pointing to them are removed.
fn sanitize_dict<K: ArrowDictionaryKeyType>(dict_arr: &DictionaryArray<K>) -> DictionaryArray<K> {
    // first determine which keys actually point to dictionary values
    let mut live_values_set = BitmapPage::new();
    let mut live_values_count = 0;
    let dict_values = dict_arr.values();
    let dict_keys = dict_arr.keys();
    for key in dict_keys {
        if let Some(key) = key {
            // TODO - is this a goofy cast? Should we do something to try to ensure this doesnt
            // get called with whatever dict key in the planet ...
            let key = key.as_usize() as u16;
            if !live_values_set.contains(key) {
                live_values_set.insert(key);
                live_values_count += 1;
                if live_values_count == dict_values.len() {
                    todo!("return because we've done it!")
                }
            }
        }
    }

    let dict_val_data = dict_values.to_data();
    // TODO real capacity
    let mut mutable_dict_val_data = MutableArrayData::new(vec![&dict_val_data], false, 100);

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

    let mut new_keys: Vec<K::Native> = vec![K::Native::default(); dict_keys.len()];
    for (i, dict_key) in dict_keys.iter().enumerate() {
        if let Some(key) = dict_key {
            let new_key = remapped_keys[key.as_usize()];
            // TODO - can I expect here?
            new_keys[i] = K::Native::from_usize(new_key).expect("");
        }
    }

    let new_keys = PrimitiveArray::<K>::from_iter_values(new_keys);
    let new_values = make_array(mutable_dict_val_data.freeze());
    DictionaryArray::new(new_keys, new_values)
}

fn default_data(arr: &ArrayRef) -> ArrayData {
    match arr.data_type() {
        DataType::UInt8 => UInt8Array::from_iter_values([0]).into_data(),
        DataType::Int64 => Int64Array::from_iter_values([0]).into_data(),
        DataType::Utf8 => StringArray::from_iter_values([""]).into_data(),
        _ => {
            todo!()
        }
    }
}

#[cfg(test)]
mod test {}
