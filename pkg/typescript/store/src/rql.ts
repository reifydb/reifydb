// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import type {FrameResults, InferShape, ShapeNode} from '@reifydb/core';
import type {SubscriptionConfig} from '@reifydb/client';

declare const paramsType: unique symbol;

export interface ReadSpec<S extends ShapeNode | readonly ShapeNode[], P extends object | null> {
    readonly kind: 'read';
    readonly rql: string;
    readonly shape: S;
    readonly config: SubscriptionConfig | undefined;
    readonly [paramsType]?: P;

    options(options: {config: SubscriptionConfig}): ReadSpec<S, P>;
}

export interface WriteSpec<S extends readonly ShapeNode[], P extends object | null> {
    readonly kind: 'write';
    readonly rql: string;
    readonly shape: S;
    readonly [paramsType]?: P;
}

export type SpecData<Spec> =
    Spec extends ReadSpec<infer S, any>
        ? S extends readonly ShapeNode[] ? FrameResults<S> : InferShape<S>[]
        : Spec extends WriteSpec<infer S, any>
            ? FrameResults<S>
            : never;

function templateText(strings: TemplateStringsArray): string {
    if (strings.length !== 1) {
        throw new Error('rql takes no ${} substitutions; write constants inline');
    }
    return strings[0];
}

function readSpec<S extends ShapeNode | readonly ShapeNode[], P extends object | null>(
    rql: string,
    shape: S,
    config: SubscriptionConfig | undefined
): ReadSpec<S, P> {
    return {
        kind: 'read',
        rql,
        shape,
        config,
        options: options => readSpec<S, P>(rql, shape, options.config),
    };
}

export function rql<const S extends ShapeNode | readonly ShapeNode[]>(shape: S) {
    return <P extends object | null = null>(strings: TemplateStringsArray): ReadSpec<S, P> =>
        readSpec<S, P>(templateText(strings), shape, undefined);
}

rql.write = <const S extends readonly ShapeNode[]>(shapes: S) =>
    <P extends object | null = null>(strings: TemplateStringsArray): WriteSpec<S, P> => ({
        kind: 'write',
        rql: templateText(strings),
        shape: shapes,
    });
