// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useEffect, useState } from 'react';
import type { Executor } from '../../types';
import { ShapeNode } from './ShapeNode';

interface ShapeBrowserProps {
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

interface NamespaceInfo {
  name: string;
  sources: SourceInfo[];
}

const TYPE_NAMES: Record<number, string> = {
  1: 'Float4', 2: 'Float8',
  3: 'Int1', 4: 'Int2', 5: 'Int4', 6: 'Int8', 7: 'Int16',
  8: 'Utf8',
  9: 'Uint1', 10: 'Uint2', 11: 'Uint4', 12: 'Uint8', 13: 'Uint16',
  14: 'Boolean', 15: 'Date', 16: 'DateTime', 17: 'Time', 18: 'Duration',
  19: 'IdentityId', 20: 'Uuid4', 21: 'Uuid7', 22: 'Blob',
  23: 'Int', 24: 'Decimal', 25: 'Uint', 26: 'Any',
  27: 'DictionaryId', 28: 'List',
};

// shape_type: 1=Table, 2=View, 3=VTable, 4=RingBuffer
const SOURCE_TYPE_TABLE = 1;
const SOURCE_TYPE_VIEW = 2;
const SOURCE_TYPE_VTABLE = 3;
const SOURCE_TYPE_RINGBUFFER = 4;

function resolveTypeName(typeId: number): string {
  const isOptional = (typeId & 0x80) !== 0;
  const baseId = typeId & 0x7f;
  const name = TYPE_NAMES[baseId] ?? `Unknown(${baseId})`;
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
      return 'rdb-shape__node-type--numeric';
    case 'Utf8': case 'Blob':
      return 'rdb-shape__node-type--string';
    case 'Boolean':
      return 'rdb-shape__node-type--boolean';
    case 'Date': case 'DateTime': case 'Time': case 'Duration':
      return 'rdb-shape__node-type--temporal';
    case 'IdentityId': case 'Uuid4': case 'Uuid7': case 'DictionaryId':
      return 'rdb-shape__node-type--identity';
    default:
      return undefined;
  }
}

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

export function ShapeBrowser({ executor }: ShapeBrowserProps) {
  const [namespaces, setNamespaces] = useState<NamespaceInfo[]>([]);
  const [loading, setLoading] = useState(true);

  const loadShape = async () => {
    setLoading(true);
    try {
      const [nsRows, tableRows, viewRows, vtableRows, rbRows, colRows, vtableColRows, procRows, handlerRows, enumRows, eventRows, dictRows, migrationRows] = await Promise.all([
        queryRows(executor, 'FROM system::namespaces MAP { id, name }'),
        queryRows(executor, 'FROM system::tables MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::views MAP { id, namespace_id, name, kind }'),
        queryRows(executor, 'FROM system::virtual_tables MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::ringbuffers MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::columns MAP { shape_id, shape_type, name, type, position }'),
        queryRows(executor, 'FROM system::virtual_table_columns MAP { vtable_id, name, type, position }'),
        queryRows(executor, 'FROM system::procedures MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::handlers MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::enums MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::events MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::dictionaries MAP { id, namespace_id, name }'),
        queryRows(executor, 'FROM system::migrations MAP { name }'),
      ]);

      // Build namespace map: id → name
      const nsMap = new Map<number, string>();
      for (const row of nsRows) {
        nsMap.set(extractNum(row.id), extractStr(row.name));
      }

      // Build column lookup: `${shape_type}:${shape_id}` → columns
      const columnsBySource = new Map<string, ColumnInfo[]>();
      // First collect with position for sorting
      const rawColumns = new Map<string, { name: string; type: string; position: number }[]>();
      for (const row of colRows) {
        const key = `${extractNum(row.shape_type)}:${extractNum(row.shape_id)}`;
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

      // Build sources grouped by namespace
      const nsSources = new Map<number, SourceInfo[]>();

      const addSource = (id: number, nsId: number, name: string, category: SourceInfo['category'], sourceType: number) => {
        if (!nsSources.has(nsId)) nsSources.set(nsId, []);
        const columns = columnsBySource.get(`${sourceType}:${id}`) ?? [];
        nsSources.get(nsId)!.push({ name: extractStr(name), category, columns });
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
        if (!nsSources.has(nsId)) nsSources.set(nsId, []);
        nsSources.get(nsId)!.push({ name: extractStr(name), category, columns: [] });
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

      // Migrations have no namespace_id — assign to "system" namespace
      let systemNsId: number | undefined;
      for (const [id, name] of nsMap) {
        if (name === 'system') { systemNsId = id; break; }
      }
      if (systemNsId !== undefined) {
        for (const row of migrationRows) {
          addLeafSource(systemNsId, extractStr(row.name), 'migration');
        }
      }

      // Build final namespace list (include all namespaces, even empty ones)
      const result: NamespaceInfo[] = [];
      for (const [nsId, nsName] of nsMap) {
        const sources = nsSources.get(nsId) ?? [];
        sources.sort((a, b) => a.name.localeCompare(b.name));
        result.push({ name: nsName, sources });
      }
      result.sort((a, b) => a.name.localeCompare(b.name));

      setNamespaces(result);
    } catch {
      setNamespaces([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadShape();
  }, [executor]); // eslint-disable-line react-hooks/exhaustive-deps

  const toolbar = (
    <div className="rdb-shape__toolbar">
      <button
        className="rdb-shape__reload-btn"
        onClick={loadShape}
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
        <div className="rdb-history__empty">$ loading shape...</div>
      </>
    );
  }

  if (namespaces.length === 0) {
    return (
      <>
        {toolbar}
        <div className="rdb-history__empty">$ no tables found</div>
      </>
    );
  }

  return (
    <>
      {toolbar}
      <div>
        {namespaces.map(ns => (
          <ShapeNode key={ns.name} label={ns.name} labelClass="rdb-shape__node-label--namespace">
            {CATEGORY_GROUPS.map(({ key, label }) => {
              const sources = ns.sources.filter(s => s.category === key);
              if (sources.length === 0) return null;
              return (
                <ShapeNode key={key} label={`${label} (${sources.length})`} labelClass="rdb-shape__node-label--category">
                  {sources.map(source => (
                    <ShapeNode key={source.name} label={source.name}>
                      {source.columns.length > 0
                        ? source.columns.map(col => (
                          <ShapeNode
                            key={col.name}
                            label={col.name}
                            labelClass="rdb-shape__node-label--column"
                            type={col.type}
                            typeClass={typeColorClass(col.type)}
                          />
                        ))
                        : undefined}
                    </ShapeNode>
                  ))}
                </ShapeNode>
              );
            })}
          </ShapeNode>
        ))}
      </div>
    </>
  );
}
