// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Recursive value-type (typeinfo) decoder. Port of crates/codec/src/typeinfo.rs.
// Produces the Display form of the ValueType (e.g. "Option(Duration)",
// "List(Any)", "Record(a: Int4)") while consuming exactly the encoded bytes.

import {
    EXTENDED_TYPE_TAG, RESERVED_KIND, TAG_DEPTH_SHIFT, TAG_KIND_MASK,
    TYPE_CODE, type_name_from_code,
} from "./format";
import { read_u16 } from "./reader";

export interface DecodedTypeInfo {
    name: string;
    next_pos: number;
}

export function decode_type_info(data: Uint8Array, pos: number): DecodedTypeInfo {
    if (pos >= data.length) throw new Error("RBCF: typeinfo truncated");
    const first = data[pos];
    pos += 1;
    if (first === EXTENDED_TYPE_TAG) {
        if (pos >= data.length) throw new Error("RBCF: extended typeinfo truncated");
        const depth = data[pos];
        pos += 1;
        const base = decode_type_info(data, pos);
        return { name: wrap_option(base.name, depth), next_pos: base.next_pos };
    }
    const kind = first & TAG_KIND_MASK;
    const depth = first >> TAG_DEPTH_SHIFT;
    if (kind === RESERVED_KIND) throw new Error(`RBCF: reserved typeinfo tag 0x${first.toString(16)}`);
    const kind_name = type_name_from_code(kind);

    let base: string;
    switch (kind) {
        case TYPE_CODE.None:
        case TYPE_CODE.Type:
            throw new Error(`RBCF: kind ${kind_name} has no standalone value type`);
        case TYPE_CODE.List: {
            const element = decode_type_info(data, pos);
            base = `List(${element.name})`;
            pos = element.next_pos;
            break;
        }
        case TYPE_CODE.Record: {
            const count = read_u16(data, pos);
            pos += 2;
            const fields: string[] = [];
            for (let i = 0; i < count; i++) {
                const name_len = read_u16(data, pos);
                pos += 2;
                const name = new TextDecoder("utf-8").decode(data.subarray(pos, pos + name_len));
                pos += name_len;
                const field = decode_type_info(data, pos);
                pos = field.next_pos;
                fields.push(`${name}: ${field.name}`);
            }
            base = `Record(${fields.join(", ")})`;
            break;
        }
        case TYPE_CODE.Tuple: {
            const count = read_u16(data, pos);
            pos += 2;
            const elements: string[] = [];
            for (let i = 0; i < count; i++) {
                const element = decode_type_info(data, pos);
                pos = element.next_pos;
                elements.push(element.name);
            }
            base = `Tuple(${elements.join(", ")})`;
            break;
        }
        default:
            base = kind_name;
    }
    return { name: wrap_option(base, depth), next_pos: pos };
}

function wrap_option(name: string, depth: number): string {
    let out = name;
    for (let i = 0; i < depth; i++) out = `Option(${out})`;
    return out;
}
