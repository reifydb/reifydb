// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Shape, Uuid7Value} from '@reifydb/core';
import {rql, type Entry, type Store} from '../../src';

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;

declare const store: Store;
declare const id: Uuid7Value;

const monitor = Shape.object({id: Shape.uuid7(), name: Shape.utf8()});
const monitorRegion = Shape.object({monitor_id: Shape.uuid7(), region_id: Shape.uuid7()});

type Monitor = {id: string; name: string};
type MonitorRegion = {monitorId: string; regionId: string};

const byId = rql(monitor)<{id: Uuid7Value}>`from uptime::monitors filter { id == $id }`;
const all = rql(monitor)`from uptime::monitors`;
const pair = rql([monitor, monitorRegion])`output from uptime::monitors; output from uptime::monitor_regions`;
const create = rql.write([monitor])<{id: Uuid7Value}>`CALL uptime::create_monitor($id); output from uptime::monitors filter { id == $id }`;

function unknownParamKeysFailOnEveryMethod() {
    // An extra key the RQL never reads is dropped silently, so a typo in a filter param would go unnoticed.

    // @ts-expect-error
    store.subscribe(byId, {id, name: id});
    // @ts-expect-error
    store.query(byId, {id, name: id});
    // @ts-expect-error
    store.getEntry(byId, {id, name: id});
    // @ts-expect-error
    store.command(create, {id, name: id});
    // @ts-expect-error
    store.admin(create, {id, name: id});
}

function paramsMustMatchTheSpecExactly() {
    // A missing or mistyped param reaches the engine as an unbound or wrong-typed variable at runtime.

    // @ts-expect-error
    store.query(byId, {});
    // @ts-expect-error
    store.query(byId, {id: 5});
    // @ts-expect-error
    store.query(byId, null);
    // @ts-expect-error
    store.query(all, {id});
}

function tupleSpecsCannotBeSubscribed() {
    // The engine allows one statement per subscription, so a tuple spec there could never hydrate.

    // @ts-expect-error
    store.subscribe(pair, null);
}

function specKindsStayOnTheirOwnMethods() {
    // A write on a read method would run a command through query rights, and a read on a write method would skip the entry.

    // @ts-expect-error
    store.query(create, {id});
    // @ts-expect-error
    store.getEntry(create, {id});
    // @ts-expect-error
    store.subscribe(create, {id});
    // @ts-expect-error
    store.command(rql([monitor])`from uptime::monitors`, null);
    // @ts-expect-error
    store.admin(rql([monitor])`from uptime::monitors`, null);
}

function returnTypesComeFromTheSpec() {
    // A result type that drifts from the spec lets a caller read a frame or field that never arrives.
    const one = store.query(byId, {id});
    const oneIsRows: Equal<typeof one, Promise<Monitor[]>> = true;

    const both = store.query(pair, null);
    const bothIsOneArrayPerFrame: Equal<typeof both, Promise<[Monitor[], MonitorRegion[]]>> = true;

    const entry = store.getEntry(byId, {id});
    const entryIsTypedByData: Equal<typeof entry, Entry<Monitor[]>> = true;
    const entryRowsIsOneMap: Equal<typeof entry.rows, ReadonlyMap<number, Monitor>> = true;
    const entryDataIsRows: Equal<typeof entry.data, Monitor[]> = true;

    const tuple = store.getEntry(pair, null);
    const tupleRowsIsOneMapPerFrame: Equal<typeof tuple.rows, [ReadonlyMap<number, Monitor>, ReadonlyMap<number, MonitorRegion>]> = true;
    const tupleDataIsOneArrayPerFrame: Equal<typeof tuple.data, [Monitor[], MonitorRegion[]]> = true;

    const created = store.command(create, {id});
    const commandIsOneArrayPerFrame: Equal<typeof created, Promise<[Monitor[]]>> = true;

    const inserted = store.command(rql.write([])`insert uptime::monitors [{ id: 1 }]`, null);
    const noShapesIsEmpty: Equal<typeof inserted, Promise<[]>> = true;

    const admined = store.admin(create, {id});
    const adminIsOneArrayPerFrame: Equal<typeof admined, Promise<[Monitor[]]>> = true;
}
