// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
export const NONE_VALUE = "⟪none⟫";

const NONE_MARKER_OPEN = NONE_VALUE.slice(0, -1);
const NONE_MARKER_CLOSE = NONE_VALUE.slice(-1);

export function noneMarker(depth: number): string {
    return depth === 0 ? NONE_VALUE : `${NONE_MARKER_OPEN}:${depth}${NONE_MARKER_CLOSE}`;
}

export function noneMarkerDepth(payload: string): number | undefined {
    if (payload === NONE_VALUE) {
        return 0;
    }
    if (!payload.startsWith(`${NONE_MARKER_OPEN}:`) || !payload.endsWith(NONE_MARKER_CLOSE)) {
        return undefined;
    }
    const digits = payload.slice(NONE_MARKER_OPEN.length + 1, -NONE_MARKER_CLOSE.length);
    return /^[1-9][0-9]*$/.test(digits) ? Number(digits) : undefined;
}

export const ROW_NUMBER_KEY = "#rownum";
