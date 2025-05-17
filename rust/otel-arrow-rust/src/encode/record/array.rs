// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};

use crate::error::Result;

pub mod string;

pub struct ArrayWithType {
    pub array: ArrayRef,
    pub data_type: DataType,
}

pub trait ArrayBuilder {
    type Native;

    fn append_value(&mut self, value: &Self::Native) -> Result<()>;

    fn finish(self) -> ArrayWithType;
}

pub trait ArrayBuilderConstructor {
    fn new() -> Self;
}

pub struct DictionaryConfig {
    // TODO fill this out
}

struct DictionaryArrayBuilder<T: ArrayBuilder + ArrayBuilderConstructor> {
    // key_builder:
    values_builder: T,
}

impl<T> DictionaryArrayBuilder<T>
where
    T: ArrayBuilder + ArrayBuilderConstructor,
{
    fn new(config: &DictionaryConfig) -> Self {
        Self {
            values_builder: T::new(),
        }
    }

    fn is_full(&self) -> bool {
        todo!()
    }
}

impl<T> ArrayBuilder for DictionaryArrayBuilder<T>
where
    T: ArrayBuilder + ArrayBuilderConstructor,
{
    type Native = T::Native;

    fn append_value(&mut self, value: &Self::Native) -> Result<()> {
        todo!()
    }

    fn finish(self) -> ArrayWithType {
        todo!();
    }
}

enum MaybeDictionaryBuilder<T: ArrayBuilder + ArrayBuilderConstructor> {
    Native(T),
    Dictionary(DictionaryArrayBuilder<T>),
}

impl<T> ArrayBuilder for MaybeDictionaryBuilder<T>
where
    T: ArrayBuilder + ArrayBuilderConstructor,
{
    type Native = T::Native;

    fn append_value(
        &mut self,
        value: &<MaybeDictionaryBuilder<T> as ArrayBuilder>::Native,
    ) -> Result<()> {
        match self {
            Self::Dictionary(dict_array_builder) => match dict_array_builder.append_value(value) {
                Ok(ok) => Ok(ok),
                Err(crate::error::Error::BuildStreamReader {
                    source: _,
                    location: _,
                }) => {
                    let mut native = T::new();
                    native.append_value(value)?;
                    *self = Self::Native(native);
                    Ok(())
                }
                Err(e) => Err(e),
            },
            Self::Native(array_builder) => array_builder.append_value(value),
        }
    }

    fn finish(self) -> ArrayWithType {
        todo!()
    }
}

#[derive(Default)]
pub struct DynamicArrayBuilderConfig {
    pub dictionary_config: Option<DictionaryConfig>,
    pub nullable: bool,
}

pub struct DynamicArrayBuilder<T: ArrayBuilder + ArrayBuilderConstructor> {
    dictionary_config: Option<DictionaryConfig>,
    inner: Option<MaybeDictionaryBuilder<T>>,
}

impl<T> DynamicArrayBuilder<T>
where
    T: ArrayBuilder + ArrayBuilderConstructor,
{
    pub fn new(config: DynamicArrayBuilderConfig) -> Self {
        todo!();
    }
}

impl<T> ArrayBuilder for DynamicArrayBuilder<T>
where
    T: ArrayBuilder + ArrayBuilderConstructor,
{
    type Native = T::Native;

    fn append_value(&mut self, value: &Self::Native) -> Result<()> {
        // TODO should this be implemented on the enum instead?

        // TODO comment
        if self.inner.is_none() {
            self.inner = match self.dictionary_config.as_ref() {
                Some(dictionary_config) => Some(MaybeDictionaryBuilder::Dictionary(
                    DictionaryArrayBuilder::new(dictionary_config),
                )),
                None => Some(MaybeDictionaryBuilder::Native(T::new())),
            };
        }

        // TODO comment
        let inner = self
            .inner
            .as_mut()
            .expect("inner should now be initialized");
        inner.append_value(value)
    }

    fn finish(self) -> ArrayWithType {
        todo!();
    }
}
