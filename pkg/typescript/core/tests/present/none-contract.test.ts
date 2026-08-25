// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {describe, expect, it} from 'vitest';
import {NONE_PRESENTATION, format_value} from '../../src/present';
import {NoneValue} from '../../src/value';
import {get_value_style} from '../../../console/src/format/value';
import {reifydb_dark_table_theme} from '../../../shell/src/output/table';

describe('missing value contract', () => {
    it('renders every form of a missing value as the shared token', () => {
        // a NoneValue used to reach String() and print its own spelling, so changing the token
        // would have moved the shell and console while leaving typed none values behind
        expect(format_value(null)).toBe(NONE_PRESENTATION.text);
        expect(format_value(undefined)).toBe(NONE_PRESENTATION.text);
        expect(format_value(new NoneValue())).toBe(NONE_PRESENTATION.text);
    });

    it('gives the shell and the console the same emphasis', () => {
        // the two surfaces each held their own italic flag; drift here is invisible until someone
        // compares a screenshot to a terminal
        const console_style = get_value_style(new NoneValue());
        const shell_style = reifydb_dark_table_theme.values?.none;

        expect(console_style.italic).toBe(NONE_PRESENTATION.italic);
        expect(shell_style?.italic).toBe(NONE_PRESENTATION.italic);
    });

    it('mutes the value on both surfaces', () => {
        const console_style = get_value_style(new NoneValue());
        const shell_style = reifydb_dark_table_theme.values?.none;

        expect(console_style.color !== undefined).toBe(NONE_PRESENTATION.muted);
        expect(shell_style?.color !== undefined).toBe(NONE_PRESENTATION.muted);
    });

    it('does not treat a literal string as a missing value', () => {
        // the token is only a rendering of absence - real text that happens to spell it stays text
        expect(get_value_style(NONE_PRESENTATION.text).italic).toBeUndefined();
    });
});
