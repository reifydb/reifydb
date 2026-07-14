// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The tag table is duplicated from Rust by hand, and two consumers (the console catalog browser
// and the react useShape hook) previously each rolled their own tag decoding and each got it
// wrong. These tests pin the one decoder they now share.

import { describe, expect, it } from "vitest";
import { TYPE_CODE, type_name_from_code, type_name_from_tag } from "../src/rbcf/format";

describe("type_name_from_code", () => {
    it("maps a bare kind byte to its name", () => {
        expect(type_name_from_code(TYPE_CODE.Int4)).toBe("Int4");
        expect(type_name_from_code(TYPE_CODE.Boolean)).toBe("Boolean");
        expect(type_name_from_code(TYPE_CODE.Vector)).toBe("Vector");
    });

    it("throws for an unassigned kind", () => {
        expect(() => type_name_from_code(33)).toThrow();
    });
});

describe("type_name_from_tag", () => {
    // A TypeTag byte is (option_depth << 6) | kind -- see type_tag_byte() in
    // crates/codec/src/tag.rs, which is what system::columns.type stores.

    it("decodes a depth-0 tag as the bare type", () => {
        expect(type_name_from_tag(TYPE_CODE.Int4)).toBe("Int4");
        expect(type_name_from_tag(TYPE_CODE.Vector)).toBe("Vector");
    });

    it("decodes an optional tag rather than reporting it as unknown", () => {
        // Option<Int4> = (1 << 6) | 6 = 70. Passing this to type_name_from_code would throw,
        // which is exactly how optional columns used to render as "Unknown(70)".
        expect(type_name_from_tag((1 << 6) | TYPE_CODE.Int4)).toBe("Int4?");
        expect(type_name_from_tag((1 << 6) | TYPE_CODE.Vector)).toBe("Vector?");
    });

    it("decodes nested option depth", () => {
        expect(type_name_from_tag((2 << 6) | TYPE_CODE.Int4)).toBe("Int4??");
        expect(type_name_from_tag((3 << 6) | TYPE_CODE.Utf8)).toBe("Utf8???");
    });

    it("does not confuse a high kind byte with optionality", () => {
        // Vector is kind 32 (0x20), which sets bit 5. The old console masked with 0x80/0x7f and
        // would have mangled any kind above 63; the depth field is bits 6-7, not bit 7 alone.
        expect(type_name_from_tag(32)).toBe("Vector");
        expect(type_name_from_tag(31)).toBe("Tuple");
    });

    it("throws for the reserved kind", () => {
        expect(() => type_name_from_tag(63)).toThrow(/reserved/);
    });

    it("throws for an unassigned kind", () => {
        expect(() => type_name_from_tag(33)).toThrow();
    });
});
