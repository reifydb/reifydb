// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
import {Shape, Uuid7Value} from '@reifydb/core';
import {rql, useAdmin, useCommand, useQuery, useSubscription, type Entry} from '../../src';

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;

declare const id: Uuid7Value;

const monitor = Shape.object({id: Shape.uuid7(), name: Shape.utf8()});
const monitorRegion = Shape.object({monitor_id: Shape.uuid7(), region_id: Shape.uuid7()});

type Monitor = {id: string; name: string};
type MonitorRegion = {monitorId: string; regionId: string};

const byId = rql(monitor)<{id: Uuid7Value}>`from uptime::monitors filter { id == $id }`;
const all = rql(monitor)`from uptime::monitors`;
const pair = rql([monitor, monitorRegion])`output from uptime::monitors; output from uptime::monitor_regions`;
const create = rql.write([monitor])<{id: Uuid7Value}>`CALL uptime::create_monitor($id); output from uptime::monitors filter { id == $id }`;
const editor = rql.write([monitor, monitorRegion])<{id: Uuid7Value}>`output from uptime::monitors filter { id == $id }; output from uptime::monitor_regions filter { monitor_id == $id }`;
const insert = rql.write([])`insert uptime::monitors [{ id: 1 }]`;

function unknownParamKeysFailOnEveryHook() {
    // An extra key the RQL never reads is dropped silently, so a typo in a filter param would go unnoticed.

    // @ts-expect-error
    useSubscription(byId, {id, name: id});
    // @ts-expect-error
    useQuery(byId, {id, name: id});
    const command = useCommand(create);
    // @ts-expect-error
    command.run({id, name: id});
    const admin = useAdmin(create);
    // @ts-expect-error
    admin.run({id, name: id});
}

function paramsMustMatchTheSpecExactly() {
    // A missing or mistyped param reaches the engine as an unbound or wrong-typed variable at runtime.

    // @ts-expect-error
    useQuery(byId, {});
    // @ts-expect-error
    useQuery(byId, {id: 5});
    // @ts-expect-error
    useQuery(byId, null);
    // @ts-expect-error
    useQuery(all, {id});
    const command = useCommand(create);
    // @ts-expect-error
    command.run(null);
    // @ts-expect-error
    command.run();
    const none = useCommand(insert);
    // @ts-expect-error
    none.run();
}

function tupleSpecsCannotBeSubscribed() {
    // The engine allows one statement per subscription, so a tuple spec there could never hydrate.

    // @ts-expect-error
    useSubscription(pair, null);
}

function specKindsStayOnTheirOwnHooks() {
    // A write on a read hook would run a command through query rights on every mount.

    // @ts-expect-error
    useQuery(create, {id});
    // @ts-expect-error
    useSubscription(create, {id});
    // @ts-expect-error
    useCommand(rql([monitor])`from uptime::monitors`);
    // @ts-expect-error
    useAdmin(rql([monitor])`from uptime::monitors`);
}

function returnTypesComeFromTheSpec() {
    // A result type that drifts from the spec lets a component read a frame or field that never arrives.
    const live = useSubscription(byId, {id});
    const subscriptionIsTypedByData: Equal<typeof live, Entry<Monitor[]>> = true;

    const one = useQuery(byId, {id});
    const queryIsTypedByData: Equal<typeof one, Entry<Monitor[]>> = true;

    const both = useQuery(pair, null);
    const tupleQueryIsTypedByData: Equal<typeof both, Entry<[Monitor[], MonitorRegion[]]>> = true;
    const tupleDataIsOneArrayPerFrame: Equal<typeof both.data, [Monitor[], MonitorRegion[]]> = true;

    const created = useCommand(create).run({id});
    const commandIsOneArrayPerFrame: Equal<typeof created, Promise<[Monitor[]]>> = true;

    const admined = useAdmin(create).run({id});
    const adminIsOneArrayPerFrame: Equal<typeof admined, Promise<[Monitor[]]>> = true;

    const inserted = useCommand(insert).run(null);
    const noShapesIsEmpty: Equal<typeof inserted, Promise<[]>> = true;
}

async function extraFramesFailToDestructure() {
    // A caller expecting a third frame would read undefined as rows without a compile error.
    const {run} = useCommand(editor);

    // @ts-expect-error
    const [monitors, regions, extra] = await run({id});
}
