// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {RecordType, TypeValuePair, Value, WireCellValue} from '.';

function hasToJSON(value: unknown): value is {toJSON(): unknown} {
    return typeof value === 'object' && value !== null && 'toJSON' in value && typeof (value as {toJSON: unknown}).toJSON === 'function';
}

export class RecordValue implements Value {
    public readonly fields: Record<string, Value>;

    constructor(fields: Record<string, Value>) {
        if (typeof fields !== 'object' || fields === null || Array.isArray(fields)) {
            throw new Error(`Record value must be a plain object of fields, got ${typeof fields}`);
        }
        this.fields = fields;
    }

    get type(): RecordType {
        return {Record: Object.entries(this.fields).map(([name, value]) => ({name, type: value.type}))};
    }

    equals(other: Value): boolean {
        if (!(other instanceof RecordValue)) {
            return false;
        }
        const names = Object.keys(this.fields);
        const otherNames = Object.keys(other.fields);
        if (names.length !== otherNames.length || names.some((name, i) => name !== otherNames[i])) {
            return false;
        }
        return names.every(name => this.fields[name].equals(other.fields[name]));
    }

    toString(): string {
        return `{${Object.entries(this.fields).map(([name, value]) => `${name}: ${value.toString()}`).join(', ')}}`;
    }

    toJSON(): unknown {
        return Object.fromEntries(Object.entries(this.fields).map(([name, value]) => [name, hasToJSON(value) ? value.toJSON() : value]));
    }

    encode(): TypeValuePair {
        const fields = Object.entries(this.fields).map(([name, value]) => ({name, type: value.type}));
        const value: {[key: string]: WireCellValue} = {};
        for (const [name, field] of Object.entries(this.fields)) {
            value[name] = field.encode().value;
        }
        return {type: {Record: fields}, value};
    }
}
