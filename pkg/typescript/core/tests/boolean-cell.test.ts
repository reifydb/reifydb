// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import { describe, expect, it } from 'vitest';
import { columnsToRows, decode } from '../src/decoder';
import { envelopesToFrames } from '../src/value';

function envelope(flag: unknown) {
    return [{ types: { flag: { id: 'Boolean' } }, rows: [{ flag }] }];
}

describe('a Boolean cell arrives as a JSON boolean', () => {
    it.each([true, false])('envelopesToFrames accepts %s without throwing', (flag) => {
        // the server sends a Boolean as a JSON bool, so the client must not demand text for it
        expect(() => envelopesToFrames(envelope(flag))).not.toThrow();
    });

    it.each([true, false])('decodes %s into a Boolean value end to end', (flag) => {
        // a bool turned into text and parsed back would hide that the wire type is a boolean
        const [frame] = envelopesToFrames(envelope(flag));
        const [row] = columnsToRows(frame.columns);
        expect(row.flag.type).toBe('Boolean');
        expect(row.flag.value).toBe(flag);
    });

    it.each([true, false])('decode accepts the bare boolean %s for a Boolean cell', (flag) => {
        // decode must take the wire value as sent, without a string detour
        expect(decode({ type: 'Boolean', value: flag as any }).value).toBe(flag);
    });

    it('still rejects a string in a Boolean column', () => {
        // the wire form of a Boolean is a JSON bool, so text there is malformed
        expect(() => envelopesToFrames(envelope('true'))).toThrow();
    });
});
