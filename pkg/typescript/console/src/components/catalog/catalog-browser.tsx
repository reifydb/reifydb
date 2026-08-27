// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useEffect, useState } from 'react';
import { typeNameFromCode } from '@reifydb/core';
import type { Executor } from '../../types';
import { useConsoleStore } from '../../state/use-console-store';
import { CatalogNode } from './catalog-node';

interface CatalogBrowserProps {
  executor: Executor;
}

interface ColumnInfo {
  name: string;
  type: string;
}

interface SourceInfo {
  name: string;
  category: 'table' | 'view' | 'vtable' | 'ringbuffer' | 'procedure' | 'handler' | 'enum' | 'event' | 'dictionary' | 'migration';
  columns: ColumnInfo[];
}

interface NamespaceTree {
  id: number;
  name: string;
  localName: string;
  sources: SourceInfo[];
  children: NamespaceTree[];
}

const SOURCE_TYPE_TABLE = 1;
const SOURCE_TYPE_VIEW = 2;
const SOURCE_TYPE_VTABLE = 3;
const SOURCE_TYPE_RINGBUFFER = 4;

function resolveTypeName(typeId: number): string {
  const isOptional = (typeId & 0x80) !== 0;
  const baseId = typeId & 0x7f;
  let name: string;
  try {
    name = typeNameFromCode(baseId);
  } catch {
    name = `Unknown(${baseId})`;
  }
  return isOptional ? `${name}?` : name;
}

function extractNum(value: unknown): number {
  if (typeof value === 'number') return value;
  if (typeof value === 'bigint') return Number(value);
  if (value && typeof value === 'object' && typeof (value as { valueOf(): unknown }).valueOf === 'function') {
    const v = (value as { valueOf(): unknown }).valueOf();
    if (typeof v === 'number') return v;
    if (typeof v === 'bigint') return Number(v);
  }
  return Number(value);
}

function extractStr(value: unknown): string {
  if (typeof value === 'string') return value;
  if (value && typeof value === 'object' && typeof (value as { valueOf(): unknown }).valueOf === 'function') {
    const v = (value as { valueOf(): unknown }).valueOf();
    if (typeof v === 'string') return v;
  }
  return String(value);
}

async function queryRows(executor: Executor, query: string): Promise<Record<string, unknown>[]> {
  const result = await executor.execute(query);
  return result.success && result.data ? result.data : [];
}

function typeColorClass(typeName: string): string | undefined {
  const base = typeName.replace(/\?$/, '');
  switch (base) {
    case 'Float4': case 'Float8':
    case 'Int1': case 'Int2': case 'Int4': case 'Int8': case 'Int16':
    case 'Uint1': case 'Uint2': case 'Uint4': case 'Uint8': case 'Uint16':
    case 'Int': case 'Uint': case 'Decimal':
      return 'rdb-catalog__node-type--numeric';
    case 'Utf8': case 'Blob':
      return 'rdb-catalog__node-type--string';
    case 'Boolean':
      return 'rdb-catalog__node-type--boolean';
    case 'Date': case 'DateTime': case 'Time': case 'Duration':
      return 'rdb-catalog__node-type--temporal';
    case 'IdentityId': case 'Uuid4': case 'Uuid7': case 'DictionaryId':
      return 'rdb-catalog__node-type--identity';
    default:
      return undefined;
  }
}

const QUERYABLE_CATEGORIES = new Set<SourceInfo['category']>(['table', 'view', 'vtable', 'ringbuffer']);

const CATEGORY_GROUPS: { key: SourceInfo['category']; label: string }[] = [
  { key: 'table', label: 'Tables' },
  { key: 'vtable', label: 'Virtual Tables' },
  { key: 'view', label: 'Views' },
  { key: 'ringbuffer', label: 'Ring Buffers' },
  { key: 'procedure', label: 'Procedures' },
  { key: 'handler', label: 'Handlers' },
  { key: 'enum', label: 'Enums' },
  { key: 'event', label: 'Events' },
  { key: 'dictionary', label: 'Dictionaries' },
  { key: 'migration', label: 'Migrations' },
];

export function CatalogBrowser({ executor }: CatalogBrowserProps) {
  const { dispatch } = useConsoleStore();
  const [roots, setRoots] = useState<NamespaceTree[]>([]);
  const [loading, setLoading] = useState(true);

  const loadCatalog = async () => {
    setLoading(true);
    try {
      const [nsRows, tableRows, viewRows, vtableRows, rbRows, colRows, vtableColRows, procRqlRows, procTestRows, procInProcessRows, procExternCRows, procExternWasmRows, handlerRows, enumRows, eventRows, dictRows, migrationRows] = await Promise.all([
        queryRows(executor, 'FROM system::namespaces MAP { id, name, local_name, parent_id }'),
        queryRows(executor, 'FROM system::tables MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::views MAP { id, namespace_id, name, kind }'),
        queryRows(executor, 'FROM system::virtual_tables MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::ringbuffers MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::columns MAP { object_id, object_type, name, type, position }'),
        queryRows(executor, 'FROM system::virtual_table_columns MAP { vtable_id, name, type, position }'),
        queryRows(executor, 'FROM system::procedures::rql MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::procedures::test MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::procedures::in_process MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::procedures::extern_c MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::procedures::extern_wasm MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::handlers MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::enums MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::events MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::dictionaries MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::migrations MAP { name }'),
      ]);
      const procRows = [...procRqlRows, ...procTestRows, ...procInProcessRows, ...procExternCRows, ...procExternWasmRows];

      const nsById = new Map<number, NamespaceTree>();
      const parentMap = new Map<number, number>();
      for (const row of nsRows) {
        const id = extractNum(row.id);
        nsById.set(id, {
          id,
          name: extractStr(row.name),
          localName: extractStr(row.local_name),
          sources: [],
          children: [],
        });
        parentMap.set(id, extractNum(row.parent_id));
      }

      const columnsBySource = new Map<string, ColumnInfo[]>();
      const rawColumns = new Map<string, { name: string; type: string; position: number }[]>();
      for (const row of colRows) {
        const key = `${extractNum(row.object_type)}:${extractNum(row.object_id)}`;
        if (!rawColumns.has(key)) rawColumns.set(key, []);
        rawColumns.get(key)!.push({
          name: extractStr(row.name),
          type: resolveTypeName(extractNum(row.type)),
          position: extractNum(row.position),
        });
      }
      for (const row of vtableColRows) {
        const key = `${SOURCE_TYPE_VTABLE}:${extractNum(row.vtable_id)}`;
        if (!rawColumns.has(key)) rawColumns.set(key, []);
        rawColumns.get(key)!.push({
          name: extractStr(row.name),
          type: resolveTypeName(extractNum(row.type)),
          position: extractNum(row.position),
        });
      }
      for (const [key, cols] of rawColumns) {
        cols.sort((a, b) => a.position - b.position);
        columnsBySource.set(key, cols.map(c => ({ name: c.name, type: c.type })));
      }

      const addSource = (id: number, nsId: number, name: string, category: SourceInfo['category'], sourceType: number) => {
        const ns = nsById.get(nsId);
        if (!ns) return;
        const columns = columnsBySource.get(`${sourceType}:${id}`) ?? [];
        ns.sources.push({ name: extractStr(name), category, columns });
      };

      for (const row of tableRows) {
        addSource(extractNum(row.id), extractNum(row.namespace_id), extractStr(row.name), 'table', SOURCE_TYPE_TABLE);
      }
      for (const row of viewRows) {
        addSource(extractNum(row.id), extractNum(row.namespace_id), extractStr(row.name), 'view', SOURCE_TYPE_VIEW);
      }
      for (const row of vtableRows) {
        addSource(extractNum(row.id), extractNum(row.namespace_id), extractStr(row.name), 'vtable', SOURCE_TYPE_VTABLE);
      }
      for (const row of rbRows) {
        addSource(extractNum(row.id), extractNum(row.namespace_id), extractStr(row.name), 'ringbuffer', SOURCE_TYPE_RINGBUFFER);
      }

      const addLeafSource = (nsId: number, name: string, category: SourceInfo['category']) => {
        const ns = nsById.get(nsId);
        if (!ns) return;
        ns.sources.push({ name: extractStr(name), category, columns: [] });
      };

      for (const row of procRows) {
        addLeafSource(extractNum(row.namespace_id), extractStr(row.name), 'procedure');
      }
      for (const row of handlerRows) {
        addLeafSource(extractNum(row.namespace_id), extractStr(row.name), 'handler');
      }
      for (const row of enumRows) {
        addLeafSource(extractNum(row.namespace_id), extractStr(row.name), 'enum');
      }
      for (const row of eventRows) {
        addLeafSource(extractNum(row.namespace_id), extractStr(row.name), 'event');
      }
      for (const row of dictRows) {
        addLeafSource(extractNum(row.namespace_id), extractStr(row.name), 'dictionary');
      }

      let systemNsId: number | undefined;
      for (const [id, ns] of nsById) {
        if (ns.name === 'system') { systemNsId = id; break; }
      }
      if (systemNsId !== undefined) {
        for (const row of migrationRows) {
          addLeafSource(systemNsId, extractStr(row.name), 'migration');
        }
      }

      const rootNodes: NamespaceTree[] = [];
      for (const [id, ns] of nsById) {
        ns.sources.sort((a, b) => a.name.localeCompare(b.name));
        const pid = parentMap.get(id) ?? 0;
        const parent = pid !== 0 ? nsById.get(pid) : undefined;
        if (parent) {
          parent.children.push(ns);
        } else {
          rootNodes.push(ns);
        }
      }
      const sortChildren = (nodes: NamespaceTree[]) => {
        nodes.sort((a, b) => a.localName.localeCompare(b.localName));
        for (const n of nodes) sortChildren(n.children);
      };
      sortChildren(rootNodes);

      setRoots(rootNodes);
    } catch {
      setRoots([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadCatalog();
  }, [executor]); // eslint-disable-line react-hooks/exhaustive-deps

  const toolbar = (
    <div className="rdb-catalog__toolbar">
      <button
        className="rdb-catalog__reload-btn"
        onClick={loadCatalog}
        disabled={loading}
      >
        {loading ? '[loading...]' : '[reload]'}
      </button>
    </div>
  );

  if (loading) {
    return (
      <>
        {toolbar}
        <div className="rdb-history__empty">$ loading catalog...</div>
      </>
    );
  }

  if (roots.length === 0) {
    return (
      <>
        {toolbar}
        <div className="rdb-history__empty">$ no tables found</div>
      </>
    );
  }

  const renderSources = (sources: SourceInfo[], namespaceName: string) =>
    CATEGORY_GROUPS.map(({ key, label }) => {
      const matching = sources.filter(s => s.category === key);
      if (matching.length === 0) return null;
      return (
        <CatalogNode key={key} label={`${label} (${matching.length})`} labelClass="rdb-catalog__node-label--category">
          {matching.map(source => (
            <CatalogNode
              key={source.name}
              label={source.name}
              onClick={QUERYABLE_CATEGORIES.has(source.category) ? () => {
                dispatch({ type: 'LOAD_QUERY', code: `FROM ${namespaceName}::${source.name}\nTAKE 10;` });
              } : undefined}
            >
              {source.columns.length > 0
                ? source.columns.map(col => (
                  <CatalogNode
                    key={col.name}
                    label={col.name}
                    labelClass="rdb-catalog__node-label--column"
                    type={col.type}
                    typeClass={typeColorClass(col.type)}
                  />
                ))
                : undefined}
            </CatalogNode>
          ))}
        </CatalogNode>
      );
    });

  const renderTree = (nodes: NamespaceTree[]) =>
    nodes.map(ns => (
      <CatalogNode key={ns.id} label={ns.localName} labelClass="rdb-catalog__node-label--namespace">
        {renderSources(ns.sources, ns.name)}
        {renderTree(ns.children)}
      </CatalogNode>
    ));

  return (
    <>
      {toolbar}
      <div>{renderTree(roots)}</div>
    </>
  );
}
