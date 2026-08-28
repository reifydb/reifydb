// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {NONE_PRESENTATION, formatValue} from '../../src/present';
import {NoneValue} from '../../src/value';
import {getValueStyle} from '../../../editor/src/format/value';
import {reifydbDarkTableTheme} from '../../../shell/src/output/table';

describe('missing value contract', () => {
    it('renders every form of a missing value as the shared token', () => {
        // a NoneValue used to reach String() and print its own spelling, so changing the token
        // would have moved the shell and console while leaving typed none values behind
        expect(formatValue(null)).toBe(NONE_PRESENTATION.text);
        expect(formatValue(undefined)).toBe(NONE_PRESENTATION.text);
        expect(formatValue(new NoneValue())).toBe(NONE_PRESENTATION.text);
    });

    it('gives the shell and the console the same emphasis', () => {
        // the two surfaces each held their own italic flag; drift here is invisible until someone
        // compares a screenshot to a terminal
        const consoleStyle = getValueStyle(new NoneValue());
        const shellStyle = reifydbDarkTableTheme.values?.none;

        expect(consoleStyle.italic).toBe(NONE_PRESENTATION.italic);
        expect(shellStyle?.italic).toBe(NONE_PRESENTATION.italic);
    });

    it('mutes the value on both surfaces', () => {
        const consoleStyle = getValueStyle(new NoneValue());
        const shellStyle = reifydbDarkTableTheme.values?.none;

        expect(consoleStyle.color !== undefined).toBe(NONE_PRESENTATION.muted);
        expect(shellStyle?.color !== undefined).toBe(NONE_PRESENTATION.muted);
    });

    it('does not treat a literal string as a missing value', () => {
        // the token is only a rendering of absence - real text that happens to spell it stays text
        expect(getValueStyle(NONE_PRESENTATION.text).italic).toBeUndefined();
    });
});
