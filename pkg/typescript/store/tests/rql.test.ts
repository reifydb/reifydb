// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {DurationValue, Shape} from '@reifydb/core';
import {rql} from '../src';

const monitor = Shape.object({id: Shape.uuid7(), name: Shape.utf8()});

describe('rql', () => {
    it('throws when a JavaScript caller passes a substitution', () => {
        // Without TypeScript the compile error is gone, so a spliced value would reach the engine as text.
        const tag = rql(monitor) as any;
        expect(() => tag`from uptime::monitors take ${5}`).toThrow('rql takes no ${} substitutions; write constants inline');
        const writeTag = rql.write([monitor]) as any;
        expect(() => writeTag`from uptime::monitors take ${5}`).toThrow('rql takes no ${} substitutions; write constants inline');
    });

    it('options returns a new spec with the config replaced and leaves the original unchanged', () => {
        // Specs are module constants shared by every caller, so a mutating options would retune them all.
        const base = rql(monitor)`from uptime::monitors`;
        const first = base.options({config: {hydration: {enabled: true, maxRows: 10}}});
        const second = first.options({config: {linger: DurationValue.parse('5s')}});

        expect(base.config).toBeUndefined();
        expect(first.config).toEqual({hydration: {enabled: true, maxRows: 10}});
        expect(second.config).toEqual({linger: DurationValue.parse('5s')});
        expect(second).not.toBe(first);
        expect([second.kind, second.rql, second.shape]).toEqual(['read', 'from uptime::monitors', monitor]);
    });
});
