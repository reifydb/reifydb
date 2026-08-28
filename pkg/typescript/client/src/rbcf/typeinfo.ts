// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import {
    EXTENDED_TYPE_TAG, RESERVED_KIND, TAG_DEPTH_SHIFT, TAG_KIND_MASK,
    TYPE_CODE, typeNameFromCode,
} from "./format";
import { readU16 } from "./reader";

export interface DecodedTypeInfo {
    name: string;
    nextPos: number;
}

export function decodeTypeInfo(data: Uint8Array, pos: number): DecodedTypeInfo {
    if (pos >= data.length) throw new Error("RBCF: typeinfo truncated");
    const first = data[pos];
    pos += 1;
    if (first === EXTENDED_TYPE_TAG) {
        if (pos >= data.length) throw new Error("RBCF: extended typeinfo truncated");
        const depth = data[pos];
        pos += 1;
        const base = decodeTypeInfo(data, pos);
        return { name: wrapOption(base.name, depth), nextPos: base.nextPos };
    }
    const kind = first & TAG_KIND_MASK;
    const depth = first >> TAG_DEPTH_SHIFT;
    if (kind === RESERVED_KIND) throw new Error(`RBCF: reserved typeinfo tag 0x${first.toString(16)}`);
    const kindName = typeNameFromCode(kind);

    let base: string;
    switch (kind) {
        case TYPE_CODE.None:
        case TYPE_CODE.Type:
            throw new Error(`RBCF: kind ${kindName} has no standalone value type`);
        case TYPE_CODE.List: {
            const element = decodeTypeInfo(data, pos);
            base = `List(${element.name})`;
            pos = element.nextPos;
            break;
        }
        case TYPE_CODE.Record: {
            const count = readU16(data, pos);
            pos += 2;
            const fields: string[] = [];
            for (let i = 0; i < count; i++) {
                const nameLen = readU16(data, pos);
                pos += 2;
                const name = new TextDecoder("utf-8").decode(data.subarray(pos, pos + nameLen));
                pos += nameLen;
                const field = decodeTypeInfo(data, pos);
                pos = field.nextPos;
                fields.push(`${name}: ${field.name}`);
            }
            base = `Record(${fields.join(", ")})`;
            break;
        }
        case TYPE_CODE.Tuple: {
            const count = readU16(data, pos);
            pos += 2;
            const elements: string[] = [];
            for (let i = 0; i < count; i++) {
                const element = decodeTypeInfo(data, pos);
                pos = element.nextPos;
                elements.push(element.name);
            }
            base = `Tuple(${elements.join(", ")})`;
            break;
        }
        default:
            base = kindName;
    }
    return { name: wrapOption(base, depth), nextPos: pos };
}

function wrapOption(name: string, depth: number): string {
    let out = name;
    for (let i = 0; i < depth; i++) out = `Option(${out})`;
    return out;
}
