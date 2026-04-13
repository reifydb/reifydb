// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Per-type formatters. Each produces the exact string form that Rust's `Display` impl
// writes for the corresponding Value, so downstream @reifydb/core decode() works unchanged.

const NANOS_PER_SECOND_BIGINT = 1_000_000_000n;
const NANOS_PER_DAY_BIGINT = 86_400n * NANOS_PER_SECOND_BIGINT;

// Howard Hinnant's civil-from-days algorithm — mirrors Date::days_since_epoch_to_ymd.
export function daysSinceEpochToYmd(days: number): { year: number; month: number; day: number } {
    const daysSinceCe = days + 719468;
    const era = Math.floor((daysSinceCe >= 0 ? daysSinceCe : daysSinceCe - 146096) / 146097);
    const doe = daysSinceCe - era * 146097;
    const yoe = Math.floor((doe - Math.floor(doe / 1460) + Math.floor(doe / 36524) - Math.floor(doe / 146096)) / 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + Math.floor(yoe / 4) - Math.floor(yoe / 100));
    const mp = Math.floor((5 * doy + 2) / 153);
    const d = doy - Math.floor((153 * mp + 2) / 5) + 1;
    const m = mp < 10 ? mp + 3 : mp - 9;
    const year = m <= 2 ? y + 1 : y;
    return { year, month: m, day: d };
}

function pad(n: number, width: number): string {
    const s = n.toString();
    return s.length >= width ? s : "0".repeat(width - s.length) + s;
}

export function formatDate(daysSinceEpoch: number): string {
    const { year, month, day } = daysSinceEpochToYmd(daysSinceEpoch);
    if (year < 0) return `-${pad(-year, 4)}-${pad(month, 2)}-${pad(day, 2)}`;
    return `${pad(year, 4)}-${pad(month, 2)}-${pad(day, 2)}`;
}

// DateTime is stored as u64 nanoseconds since Unix epoch.
export function formatDateTime(nanos: bigint): string {
    const days = Number(nanos / NANOS_PER_DAY_BIGINT);
    const nanosOfDay = nanos % NANOS_PER_DAY_BIGINT;
    return `${formatDate(days)}T${formatTime(nanosOfDay)}Z`;
}

// Time is stored as u64 nanos since midnight.
export function formatTime(nanos: bigint): string {
    const totalSec = nanos / NANOS_PER_SECOND_BIGINT;
    const ns = Number(nanos % NANOS_PER_SECOND_BIGINT);
    const h = Number(totalSec / 3600n);
    const m = Number((totalSec % 3600n) / 60n);
    const s = Number(totalSec % 60n);
    return `${pad(h, 2)}:${pad(m, 2)}:${pad(s, 2)}.${pad(ns, 9)}`;
}

// Duration stores months (i32), days (i32), nanos (i64). Matches Display in duration.rs.
export function formatDuration(months: number, days: number, nanos: bigint): string {
    if (months === 0 && days === 0 && nanos === 0n) return "0s";

    const years = (months / 12) | 0;
    const monthsRem = (months % 12) | 0;

    const totalSeconds = nanos / NANOS_PER_SECOND_BIGINT;
    const remainingNanos = nanos % NANOS_PER_SECOND_BIGINT;

    const extraDays = Number(totalSeconds / 86400n);
    const remainingSeconds = totalSeconds % 86400n;

    const displayDays = days + extraDays;
    const hours = Number(remainingSeconds / 3600n);
    const minutes = Number((remainingSeconds % 3600n) / 60n);
    const seconds = Number(remainingSeconds % 60n);

    const absRem = remainingNanos < 0n ? -remainingNanos : remainingNanos;
    const ms = Number(absRem / 1_000_000n);
    const us = Number((absRem % 1_000_000n) / 1_000n);
    const ns = Number(absRem % 1_000n);

    let out = "";
    if (years !== 0) out += `${years}y`;
    if (monthsRem !== 0) out += `${monthsRem}mo`;
    if (displayDays !== 0) out += `${displayDays}d`;
    if (hours !== 0) out += `${hours}h`;
    if (minutes !== 0) out += `${minutes}m`;
    if (seconds !== 0) out += `${seconds}s`;

    if (ms !== 0 || us !== 0 || ns !== 0) {
        if (
            remainingNanos < 0n &&
            seconds === 0 &&
            hours === 0 &&
            minutes === 0 &&
            displayDays === 0 &&
            years === 0 &&
            monthsRem === 0
        ) {
            out += "-";
        }
        if (ms !== 0) out += `${ms}ms`;
        if (us !== 0) out += `${us}us`;
        if (ns !== 0) out += `${ns}ns`;
    }

    return out;
}

// UUID canonical hyphenated form from 16 bytes.
export function formatUuid(bytes: Uint8Array): string {
    if (bytes.length !== 16) throw new Error(`uuid requires 16 bytes, got ${bytes.length}`);
    const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

export function formatBlob(bytes: Uint8Array): string {
    let hex = "";
    for (const b of bytes) hex += b.toString(16).padStart(2, "0");
    return `0x${hex}`;
}

// Arbitrary-precision signed integer from little-endian two's-complement bytes.
// Mirrors num_bigint::BigInt::from_signed_bytes_le.
export function signedBigIntFromLeBytes(bytes: Uint8Array): bigint {
    if (bytes.length === 0) return 0n;
    // Build unsigned from little-endian
    let u = 0n;
    for (let i = bytes.length - 1; i >= 0; i--) {
        u = (u << 8n) | BigInt(bytes[i]);
    }
    // Sign-extend if top bit of highest byte is set
    const highBit = bytes[bytes.length - 1] & 0x80;
    if (highBit) {
        const bits = BigInt(bytes.length) * 8n;
        u -= 1n << bits;
    }
    return u;
}

// Convert a bigint to little-endian two's-complement bytes (minimal length).
export function signedBigIntToLeBytes(v: bigint): Uint8Array {
    if (v === 0n) return new Uint8Array(0);
    const negative = v < 0n;
    // Iteratively emit bytes LSB-first; stop when the remaining value matches
    // the sign-extension of the last emitted byte.
    const out: number[] = [];
    let cur = v;
    while (true) {
        const byte = Number(cur & 0xffn);
        out.push(byte);
        const nextCur = cur >> 8n; // arithmetic shift for bigint
        const signBit = byte & 0x80 ? 1 : 0;
        const done = negative
            ? nextCur === -1n && signBit === 1
            : nextCur === 0n && signBit === 0;
        if (done) break;
        cur = nextCur;
    }
    return Uint8Array.from(out);
}

// Rust f32/f64 Display: uses shortest round-trip representation.
// JS toString is close but differs for some edge values; for our purposes of feeding
// the result back into parseFloat (TS decoder), this is good enough.
export function formatF32(v: number): string {
    if (Number.isNaN(v)) return "NaN";
    if (v === Infinity) return "inf";
    if (v === -Infinity) return "-inf";
    // Rust default: if the value is an integer, print as "N" (no decimal); else shortest round-trip.
    // JS's Number.prototype.toString() already does shortest round-trip; for integers it prints
    // "1" (not "1.0"), which matches Rust default Display. For negative zero, both produce "-0".
    return v.toString();
}

export function formatF64(v: number): string {
    return formatF32(v);
}
