// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expectTypeOf, it} from 'vitest';
import {Shape} from '../../src/shape/builder';
import {InferShape} from '../../src/shape';
import {Int4Value, Option} from '../../src/value';

describe('InferShape', () => {
    it('maps a nested option to a nested Option type', () => {
        const shape = Shape.option(Shape.option(Shape.int4Value()));
        expectTypeOf<InferShape<typeof shape>>().toEqualTypeOf<Option<Option<Int4Value>>>();
        const row = Shape.object({n: Shape.option(Shape.int4())});
        expectTypeOf<InferShape<typeof row>>().toEqualTypeOf<{n: Option<number>}>();
    });
});
