// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {display_width, pad_to_width, truncate_to_width} from '../../src/present';

describe('display_width', () => {
    it('counts ascii as one cell each', () => {
        expect(display_width('alice')).toBe(5);
    });

    it('counts cjk as two cells', () => {
        // a table padded by code-unit length puts these rows one cell short and misaligns every row below
        expect(display_width('東京')).toBe(4);
        expect('東京'.length).toBe(2);
    });

    it('counts an emoji as two cells, not as one code point', () => {
        expect(display_width('🚀')).toBe(2);
        expect(Array.from('🚀')).toHaveLength(1);
    });

    it('counts a combining mark as part of its base character', () => {
        expect(display_width('é')).toBe(1);
    });

    it('ignores zero-width joiners inside one grapheme', () => {
        expect(display_width('👩‍💻')).toBe(2);
    });
});

describe('pad_to_width', () => {
    it('pads wide text to the requested cell count, not character count', () => {
        expect(display_width(pad_to_width('東京', 6, 'left'))).toBe(6);
    });

    it('pads right-aligned text on the left', () => {
        expect(pad_to_width('34', 5, 'right')).toBe('   34');
    });

    it('leaves text that already fills the width untouched', () => {
        expect(pad_to_width('abcde', 5, 'left')).toBe('abcde');
    });

    it('never widens text past the requested width', () => {
        expect(pad_to_width('abcdefgh', 5, 'left')).toBe('abcdefgh');
    });
});

describe('truncate_to_width', () => {
    it('keeps text that fits', () => {
        expect(truncate_to_width('alice', 10)).toBe('alice');
    });

    it('produces exactly the requested cell count when it truncates', () => {
        const result = truncate_to_width('administrator', 6);
        expect(display_width(result)).toBe(6);
        expect(result.endsWith('…')).toBe(true);
    });

    it('does not split a wide character across the boundary', () => {
        // splitting a two-cell character would leave the row one cell narrow
        const result = truncate_to_width('東京都港区', 5);
        expect(display_width(result)).toBeLessThanOrEqual(5);
        expect(result.endsWith('…')).toBe(true);
    });
});
