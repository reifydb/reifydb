// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

const ZERO_WIDTH: ReadonlyArray<readonly [number, number]> = [
    [0x0300, 0x036f],
    [0x0483, 0x0489],
    [0x0591, 0x05bd],
    [0x0610, 0x061a],
    [0x064b, 0x065f],
    [0x0e31, 0x0e31],
    [0x0e34, 0x0e3a],
    [0x0eb1, 0x0eb1],
    [0x0eb4, 0x0eb9],
    [0x200b, 0x200f],
    [0x20d0, 0x20ff],
    [0xfe00, 0xfe0f],
    [0xfe20, 0xfe2f],
    [0xfeff, 0xfeff],
];

const WIDE: ReadonlyArray<readonly [number, number]> = [
    [0x1100, 0x115f],
    [0x2329, 0x232a],
    [0x2e80, 0x303e],
    [0x3041, 0x33ff],
    [0x3400, 0x4dbf],
    [0x4e00, 0x9fff],
    [0xa000, 0xa4cf],
    [0xa960, 0xa97f],
    [0xac00, 0xd7a3],
    [0xf900, 0xfaff],
    [0xfe10, 0xfe19],
    [0xfe30, 0xfe6f],
    [0xff00, 0xff60],
    [0xffe0, 0xffe6],
    [0x1f300, 0x1f9ff],
    [0x1fa70, 0x1faff],
    [0x20000, 0x3fffd],
];

function in_ranges(code: number, ranges: ReadonlyArray<readonly [number, number]>): boolean {
    let low = 0;
    let high = ranges.length - 1;
    while (low <= high) {
        const middle = (low + high) >> 1;
        const [start, end] = ranges[middle];
        if (code < start) high = middle - 1;
        else if (code > end) low = middle + 1;
        else return true;
    }
    return false;
}

function code_point_width(code: number): number {
    if (code === 0) return 0;
    if (code < 0x20 || (code >= 0x7f && code < 0xa0)) return 0;
    if (in_ranges(code, ZERO_WIDTH)) return 0;
    if (in_ranges(code, WIDE)) return 2;
    return 1;
}

interface GraphemeSegmenter {
    segment(input: string): Iterable<{segment: string}>;
}

type GraphemeSegmenterFactory = new (
    locale: string | undefined,
    options: {granularity: 'grapheme'},
) => GraphemeSegmenter;

function create_segmenter(): GraphemeSegmenter | null {
    if (typeof Intl === 'undefined') return null;
    const factory = (Intl as {Segmenter?: GraphemeSegmenterFactory}).Segmenter;
    if (typeof factory !== 'function') return null;
    return new factory(undefined, {granularity: 'grapheme'});
}

const segmenter = create_segmenter();

function graphemes(text: string): string[] {
    if (segmenter !== null) {
        return Array.from(segmenter.segment(text), (entry) => entry.segment);
    }
    return Array.from(text);
}

export function grapheme_width(grapheme: string): number {
    let width = 0;
    for (const character of grapheme) {
        width += code_point_width(character.codePointAt(0) ?? 0);
    }
    return Math.min(width, 2);
}

export function display_width(text: string): number {
    let width = 0;
    for (const grapheme of graphemes(text)) {
        width += grapheme_width(grapheme);
    }
    return width;
}

export function truncate_to_width(text: string, width: number, ellipsis = '…'): string {
    if (width <= 0) return '';
    if (display_width(text) <= width) return text;

    const marker_width = display_width(ellipsis);
    if (width <= marker_width) return ellipsis.slice(0, width);

    const budget = width - marker_width;
    let used = 0;
    let result = '';
    for (const grapheme of graphemes(text)) {
        const next = grapheme_width(grapheme);
        if (used + next > budget) break;
        result += grapheme;
        used += next;
    }

    return result + ellipsis;
}

export function pad_to_width(text: string, width: number, align: 'left' | 'right' | 'center'): string {
    const padding = width - display_width(text);
    if (padding <= 0) return text;

    if (align === 'right') return ' '.repeat(padding) + text;
    if (align === 'left') return text + ' '.repeat(padding);

    const left = Math.floor(padding / 2);
    return ' '.repeat(left) + text + ' '.repeat(padding - left);
}
