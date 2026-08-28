// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {displayWidth, padToWidth, truncateToWidth} from '../../src/present';

describe('displayWidth', () => {
    it('counts ascii as one cell each', () => {
        expect(displayWidth('alice')).toBe(5);
    });

    it('counts cjk as two cells', () => {
        // a table padded by code-unit length puts these rows one cell short and misaligns every row below
        expect(displayWidth('東京')).toBe(4);
        expect('東京'.length).toBe(2);
    });

    it('counts an emoji as two cells, not as one code point', () => {
        expect(displayWidth('🚀')).toBe(2);
        expect(Array.from('🚀')).toHaveLength(1);
    });

    it('counts a combining mark as part of its base character', () => {
        expect(displayWidth('é')).toBe(1);
    });

    it('ignores zero-width joiners inside one grapheme', () => {
        expect(displayWidth('👩‍💻')).toBe(2);
    });
});

describe('padToWidth', () => {
    it('pads wide text to the requested cell count, not character count', () => {
        expect(displayWidth(padToWidth('東京', 6, 'left'))).toBe(6);
    });

    it('pads right-aligned text on the left', () => {
        expect(padToWidth('34', 5, 'right')).toBe('   34');
    });

    it('leaves text that already fills the width untouched', () => {
        expect(padToWidth('abcde', 5, 'left')).toBe('abcde');
    });

    it('never widens text past the requested width', () => {
        expect(padToWidth('abcdefgh', 5, 'left')).toBe('abcdefgh');
    });
});

describe('truncateToWidth', () => {
    it('keeps text that fits', () => {
        expect(truncateToWidth('alice', 10)).toBe('alice');
    });

    it('produces exactly the requested cell count when it truncates', () => {
        const result = truncateToWidth('administrator', 6);
        expect(displayWidth(result)).toBe(6);
        expect(result.endsWith('…')).toBe(true);
    });

    it('does not split a wide character across the boundary', () => {
        // splitting a two-cell character would leave the row one cell narrow
        const result = truncateToWidth('東京都港区', 5);
        expect(displayWidth(result)).toBeLessThanOrEqual(5);
        expect(result.endsWith('…')).toBe(true);
    });
});
