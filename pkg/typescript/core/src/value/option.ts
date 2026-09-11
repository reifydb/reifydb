// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {OptionType, Type, isOptionType} from '.';
import {NONE_PRESENTATION} from '../present/value';

type State<T> = {some: true; value: T} | {some: false; inner: Type};

function describeType(t: Type): string {
    return isOptionType(t) ? `Option(${describeType(t.Option)})` : t;
}

function hasType(value: unknown): value is {type: Type} {
    return typeof value === 'object' && value !== null && 'type' in value;
}

function hasToJSON(value: unknown): value is {toJSON(): unknown} {
    return typeof value === 'object' && value !== null && 'toJSON' in value && typeof value.toJSON === 'function';
}

// a Symbol.for brand survives a second copy of this module, where instanceof would not
const OPTION_BRAND = Symbol.for('reifydb.option');

export function isOption(value: unknown): value is Option<unknown> {
    return typeof value === 'object' && value !== null && (value as Record<symbol, unknown>)[OPTION_BRAND] === true;
}

export class Option<T> {
    readonly [OPTION_BRAND] = true;

    private constructor(private readonly state: State<T>) {}

    static some<T>(value: T): Option<T> {
        return new Option<T>({some: true, value});
    }

    static none<T = never>(inner: Type): Option<T> {
        return new Option<T>({some: false, inner});
    }

    isSome(): boolean {
        return this.state.some;
    }

    isNone(): boolean {
        return !this.state.some;
    }

    unwrap(): T {
        if (this.state.some) {
            return this.state.value;
        }
        throw new Error(`unwrap on a None of type ${describeType(this.state.inner)}`);
    }

    unwrapOr(fallback: T): T {
        return this.state.some ? this.state.value : fallback;
    }

    map<U>(fn: (value: T) => U): Option<U> {
        // a None carries no U, so the missing type stays the one it was created with
        return this.state.some ? Option.some(fn(this.state.value)) : Option.none<U>(this.state.inner);
    }

    get type(): OptionType {
        if (!this.state.some) {
            return {Option: this.state.inner};
        }
        if (hasType(this.state.value)) {
            return {Option: this.state.value.type};
        }
        throw new Error(`Option.some over a raw ${typeof this.state.value} has no wire type`);
    }

    toString(): string {
        return this.state.some ? String(this.state.value) : NONE_PRESENTATION.text;
    }

    toJSON(): unknown {
        if (!this.state.some) {
            return null;
        }
        // JSON.stringify serializes what toJSON returns as is, so nested toJSON must be applied here
        return hasToJSON(this.state.value) ? this.state.value.toJSON() : this.state.value;
    }
}
