// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::{ArrayBuilder, ArrayBuilderConstructor};
use crate::error::Result;

pub struct StringArrayBuilder {}

impl ArrayBuilderConstructor for StringArrayBuilder {
    fn new() -> Self {
        todo!()
    }
}

impl ArrayBuilder for StringArrayBuilder {
    type Native = String;

    fn append_value(&mut self, _value: &Self::Native) -> Result<()> {
        todo!()
    }

    fn finish(self) -> super::ArrayWithType {
        todo!()
    }
}
