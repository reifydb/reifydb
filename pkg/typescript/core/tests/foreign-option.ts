// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {OptionType, Type} from '../src/value';

type State<T> = {some: true; value: T} | {some: false; inner: Type};

// a second Option class carrying the brand, standing in for core loaded twice in one process
export class ForeignOption<T> {
    readonly [Symbol.for('reifydb.option')] = true;

    private constructor(private readonly state: State<T>) {}

    static some<T>(value: T): ForeignOption<T> {
        return new ForeignOption<T>({some: true, value});
    }

    static none<T = never>(inner: Type): ForeignOption<T> {
        return new ForeignOption<T>({some: false, inner});
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
        throw new Error('unwrap on a None');
    }

    map<U>(fn: (value: T) => U): ForeignOption<U> {
        return this.state.some ? ForeignOption.some(fn(this.state.value)) : ForeignOption.none<U>(this.state.inner);
    }

    get type(): OptionType {
        if (!this.state.some) {
            return {Option: this.state.inner};
        }
        return {Option: (this.state.value as {type: Type}).type};
    }
}

// the same shape without the brand, so shape alone must not pass as an Option
export function unbrandedOption<T>(value: T): {isSome(): boolean; isNone(): boolean; unwrap(): T} {
    return {
        isSome: () => true,
        isNone: () => false,
        unwrap: () => value,
    };
}
