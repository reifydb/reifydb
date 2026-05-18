// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Per-type formatters. Each produces the exact string form that Rust's `Display` impl
// writes for the corresponding Value, so downstream @reifydb/core decode() works unchanged.

const NANOS_PER_SECOND_BIGINT = 1_000_000_000n;
const NANOS_PER_DAY_BIGINT = 86_400n * NANOS_PER_SECOND_BIGINT;

// Howard Hinnant's civil-from-days algorithm — mirrors Date::days_since_epoch_to_ymd.
export function days_since_epoch_to_ymd(days: number): { year: number; month: number; day: number } {
    const days_since_ce = days + 719468;
    const era = Math.floor((days_since_ce >= 0 ? days_since_ce : days_since_ce - 146096) / 146097);
    const doe = days_since_ce - era * 146097;
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

export function format_date(days_since_epoch: number): string {
    const { year, month, day } = days_since_epoch_to_ymd(days_since_epoch);
    if (year < 0) return `-${pad(-year, 4)}-${pad(month, 2)}-${pad(day, 2)}`;
    return `${pad(year, 4)}-${pad(month, 2)}-${pad(day, 2)}`;
}

// DateTime is stored as u64 nanoseconds since Unix epoch.
export function format_date_time(nanos: bigint): string {
    const days = Number(nanos / NANOS_PER_DAY_BIGINT);
    const nanos_of_day = nanos % NANOS_PER_DAY_BIGINT;
    return `${format_date(days)}T${format_time(nanos_of_day)}Z`;
}

// Time is stored as u64 nanos since midnight.
export function format_time(nanos: bigint): string {
    const total_sec = nanos / NANOS_PER_SECOND_BIGINT;
    const ns = Number(nanos % NANOS_PER_SECOND_BIGINT);
    const h = Number(total_sec / 3600n);
    const m = Number((total_sec % 3600n) / 60n);
    const s = Number(total_sec % 60n);
    return `${pad(h, 2)}:${pad(m, 2)}:${pad(s, 2)}.${pad(ns, 9)}`;
}

// Duration stores months (i32), days (i32), nanos (i64). Matches Display in duration.rs.
export function format_duration(months: number, days: number, nanos: bigint): string {
    if (months === 0 && days === 0 && nanos === 0n) return "0s";

    const years = (months / 12) | 0;
    const months_rem = (months % 12) | 0;

    const total_seconds = nanos / NANOS_PER_SECOND_BIGINT;
    const remaining_nanos = nanos % NANOS_PER_SECOND_BIGINT;

    const extra_days = Number(total_seconds / 86400n);
    const remaining_seconds = total_seconds % 86400n;

    const display_days = days + extra_days;
    const hours = Number(remaining_seconds / 3600n);
    const minutes = Number((remaining_seconds % 3600n) / 60n);
    const seconds = Number(remaining_seconds % 60n);

    const abs_rem = remaining_nanos < 0n ? -remaining_nanos : remaining_nanos;
    const ms = Number(abs_rem / 1_000_000n);
    const us = Number((abs_rem % 1_000_000n) / 1_000n);
    const ns = Number(abs_rem % 1_000n);

    let out = "";
    if (years !== 0) out += `${years}y`;
    if (months_rem !== 0) out += `${months_rem}mo`;
    if (display_days !== 0) out += `${display_days}d`;
    if (hours !== 0) out += `${hours}h`;
    if (minutes !== 0) out += `${minutes}m`;
    if (seconds !== 0) out += `${seconds}s`;

    if (ms !== 0 || us !== 0 || ns !== 0) {
        if (
            remaining_nanos < 0n &&
            seconds === 0 &&
            hours === 0 &&
            minutes === 0 &&
            display_days === 0 &&
            years === 0 &&
            months_rem === 0
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
export function format_uuid(bytes: Uint8Array): string {
    if (bytes.length !== 16) throw new Error(`uuid requires 16 bytes, got ${bytes.length}`);
    const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

export function format_blob(bytes: Uint8Array): string {
    let hex = "";
    for (const b of bytes) hex += b.toString(16).padStart(2, "0");
    return `0x${hex}`;
}

// Arbitrary-precision signed integer from little-endian two's-complement bytes.
// Mirrors num_bigint::BigInt::from_signed_bytes_le.
export function signed_big_int_from_le_bytes(bytes: Uint8Array): bigint {
    if (bytes.length === 0) return 0n;
    // Build unsigned from little-endian
    let u = 0n;
    for (let i = bytes.length - 1; i >= 0; i--) {
        u = (u << 8n) | BigInt(bytes[i]);
    }
    // Sign-extend if top bit of highest byte is set
    const high_bit = bytes[bytes.length - 1] & 0x80;
    if (high_bit) {
        const bits = BigInt(bytes.length) * 8n;
        u -= 1n << bits;
    }
    return u;
}

// Convert a bigint to little-endian two's-complement bytes (minimal length).
export function signed_big_int_to_le_bytes(v: bigint): Uint8Array {
    if (v === 0n) return new Uint8Array(0);
    const negative = v < 0n;
    // Iteratively emit bytes LSB-first; stop when the remaining value matches
    // the sign-extension of the last emitted byte.
    const out: number[] = [];
    let cur = v;
    while (true) {
        const byte = Number(cur & 0xffn);
        out.push(byte);
        const next_cur = cur >> 8n; // arithmetic shift for bigint
        const sign_bit = byte & 0x80 ? 1 : 0;
        const done = negative
            ? next_cur === -1n && sign_bit === 1
            : next_cur === 0n && sign_bit === 0;
        if (done) break;
        cur = next_cur;
    }
    return Uint8Array.from(out);
}

// Rust f32/f64 Display: uses shortest round-trip representation.
// JS toString is close but differs for some edge values; for our purposes of feeding
// the result back into parseFloat (TS decoder), this is good enough.
export function format_f32(v: number): string {
    if (Number.isNaN(v)) return "NaN";
    if (v === Infinity) return "inf";
    if (v === -Infinity) return "-inf";
    // Rust default: if the value is an integer, print as "N" (no decimal); else shortest round-trip.
    // JS's Number.prototype.toString() already does shortest round-trip; for integers it prints
    // "1" (not "1.0"), which matches Rust default Display. For negative zero, both produce "-0".
    return v.toString();
}

export function format_f64(v: number): string {
    return format_f32(v);
}
