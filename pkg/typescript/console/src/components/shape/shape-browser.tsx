// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useEffect, useState } from 'react';
import type { Executor } from '../../types';
import { ShapeNode } from './shape-node';

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

interface NamespaceTree {
  id: number;
  name: string;
  local_name: string;
  sources: SourceInfo[];
  children: NamespaceTree[];
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

function resolve_type_name(type_id: number): string {
  const is_optional = (type_id & 0x80) !== 0;
  const base_id = type_id & 0x7f;
  const name = TYPE_NAMES[base_id] ?? `Unknown(${base_id})`;
  return is_optional ? `${name}?` : name;
}

function extract_num(value: unknown): number {
  if (typeof value === 'number') return value;
  if (typeof value === 'bigint') return Number(value);
  if (value && typeof value === 'object' && typeof (value as { valueOf(): unknown }).valueOf === 'function') {
    const v = (value as { valueOf(): unknown }).valueOf();
    if (typeof v === 'number') return v;
    if (typeof v === 'bigint') return Number(v);
  }
  return Number(value);
}

function extract_str(value: unknown): string {
  if (typeof value === 'string') return value;
  if (value && typeof value === 'object' && typeof (value as { valueOf(): unknown }).valueOf === 'function') {
    const v = (value as { valueOf(): unknown }).valueOf();
    if (typeof v === 'string') return v;
  }
  return String(value);
}

async function query_rows(executor: Executor, query: string): Promise<Record<string, unknown>[]> {
  const result = await executor.execute(query);
  return result.success && result.data ? result.data : [];
}

function type_color_class(type_name: string): string | undefined {
  const base = type_name.replace(/\?$/, '');
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
  const [roots, setRoots] = useState<NamespaceTree[]>([]);
  const [loading, setLoading] = useState(true);

  const load_shape = async () => {
    setLoading(true);
    try {
      const [ns_rows, table_rows, view_rows, vtable_rows, rb_rows, col_rows, vtable_col_rows, proc_rql_rows, proc_test_rows, proc_native_rows, proc_ffi_rows, proc_wasm_rows, handler_rows, enum_rows, event_rows, dict_rows, migration_rows] = await Promise.all([
        query_rows(executor, 'FROM system::namespaces MAP { id, name, local_name, parent_id }'),
        query_rows(executor, 'FROM system::tables MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::views MAP { id, namespace_id, name, kind }'),
        query_rows(executor, 'FROM system::virtual_tables MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::ringbuffers MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::columns MAP { shape_id, shape_type, name, type, position }'),
        query_rows(executor, 'FROM system::virtual_table_columns MAP { vtable_id, name, type, position }'),
        query_rows(executor, 'FROM system::procedures::rql MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::procedures::test MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::procedures::native MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::procedures::ffi MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::procedures::wasm MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::handlers MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::enums MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::events MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::dictionaries MAP { id, namespace_id, name }'),
        query_rows(executor, 'FROM system::migrations MAP { name }'),
      ]);
      const proc_rows = [...proc_rql_rows, ...proc_test_rows, ...proc_native_rows, ...proc_ffi_rows, ...proc_wasm_rows];

      // Build namespace tree nodes: id → NamespaceTree
      const ns_by_id = new Map<number, NamespaceTree>();
      const parent_map = new Map<number, number>(); // id → parent_id
      for (const row of ns_rows) {
        const id = extract_num(row.id);
        ns_by_id.set(id, {
          id,
          name: extract_str(row.name),
          local_name: extract_str(row.local_name),
          sources: [],
          children: [],
        });
        parent_map.set(id, extract_num(row.parent_id));
      }

      // Build column lookup: `${shape_type}:${shape_id}` → columns
      const columns_by_source = new Map<string, ColumnInfo[]>();
      // First collect with position for sorting
      const raw_columns = new Map<string, { name: string; type: string; position: number }[]>();
      for (const row of col_rows) {
        const key = `${extract_num(row.shape_type)}:${extract_num(row.shape_id)}`;
        if (!raw_columns.has(key)) raw_columns.set(key, []);
        raw_columns.get(key)!.push({
          name: extract_str(row.name),
          type: resolve_type_name(extract_num(row.type)),
          position: extract_num(row.position),
        });
      }
      for (const row of vtable_col_rows) {
        const key = `${SOURCE_TYPE_VTABLE}:${extract_num(row.vtable_id)}`;
        if (!raw_columns.has(key)) raw_columns.set(key, []);
        raw_columns.get(key)!.push({
          name: extract_str(row.name),
          type: resolve_type_name(extract_num(row.type)),
          position: extract_num(row.position),
        });
      }
      for (const [key, cols] of raw_columns) {
        cols.sort((a, b) => a.position - b.position);
        columns_by_source.set(key, cols.map(c => ({ name: c.name, type: c.type })));
      }

      // Add sources directly to namespace tree nodes
      const add_source = (id: number, ns_id: number, name: string, category: SourceInfo['category'], source_type: number) => {
        const ns = ns_by_id.get(ns_id);
        if (!ns) return;
        const columns = columns_by_source.get(`${source_type}:${id}`) ?? [];
        ns.sources.push({ name: extract_str(name), category, columns });
      };

      for (const row of table_rows) {
        add_source(extract_num(row.id), extract_num(row.namespace_id), extract_str(row.name), 'table', SOURCE_TYPE_TABLE);
      }
      for (const row of view_rows) {
        add_source(extract_num(row.id), extract_num(row.namespace_id), extract_str(row.name), 'view', SOURCE_TYPE_VIEW);
      }
      for (const row of vtable_rows) {
        add_source(extract_num(row.id), extract_num(row.namespace_id), extract_str(row.name), 'vtable', SOURCE_TYPE_VTABLE);
      }
      for (const row of rb_rows) {
        add_source(extract_num(row.id), extract_num(row.namespace_id), extract_str(row.name), 'ringbuffer', SOURCE_TYPE_RINGBUFFER);
      }

      const add_leaf_source = (ns_id: number, name: string, category: SourceInfo['category']) => {
        const ns = ns_by_id.get(ns_id);
        if (!ns) return;
        ns.sources.push({ name: extract_str(name), category, columns: [] });
      };

      for (const row of proc_rows) {
        add_leaf_source(extract_num(row.namespace_id), extract_str(row.name), 'procedure');
      }
      for (const row of handler_rows) {
        add_leaf_source(extract_num(row.namespace_id), extract_str(row.name), 'handler');
      }
      for (const row of enum_rows) {
        add_leaf_source(extract_num(row.namespace_id), extract_str(row.name), 'enum');
      }
      for (const row of event_rows) {
        add_leaf_source(extract_num(row.namespace_id), extract_str(row.name), 'event');
      }
      for (const row of dict_rows) {
        add_leaf_source(extract_num(row.namespace_id), extract_str(row.name), 'dictionary');
      }

      // Migrations have no namespace_id — assign to "system" namespace
      let system_ns_id: number | undefined;
      for (const [id, ns] of ns_by_id) {
        if (ns.name === 'system') { system_ns_id = id; break; }
      }
      if (system_ns_id !== undefined) {
        for (const row of migration_rows) {
          add_leaf_source(system_ns_id, extract_str(row.name), 'migration');
        }
      }

      // Build tree from parent_id relationships
      const root_nodes: NamespaceTree[] = [];
      for (const [id, ns] of ns_by_id) {
        ns.sources.sort((a, b) => a.name.localeCompare(b.name));
        const pid = parent_map.get(id) ?? 0;
        const parent = pid !== 0 ? ns_by_id.get(pid) : undefined;
        if (parent) {
          parent.children.push(ns);
        } else {
          root_nodes.push(ns);
        }
      }
      // Sort children at every level
      const sort_children = (nodes: NamespaceTree[]) => {
        nodes.sort((a, b) => a.local_name.localeCompare(b.local_name));
        for (const n of nodes) sort_children(n.children);
      };
      sort_children(root_nodes);

      setRoots(root_nodes);
    } catch {
      setRoots([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load_shape();
  }, [executor]); // eslint-disable-line react-hooks/exhaustive-deps

  const toolbar = (
    <div className="rdb-shape__toolbar">
      <button
        className="rdb-shape__reload-btn"
        onClick={load_shape}
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

  if (roots.length === 0) {
    return (
      <>
        {toolbar}
        <div className="rdb-history__empty">$ no tables found</div>
      </>
    );
  }

  const render_sources = (sources: SourceInfo[]) =>
    CATEGORY_GROUPS.map(({ key, label }) => {
      const matching = sources.filter(s => s.category === key);
      if (matching.length === 0) return null;
      return (
        <ShapeNode key={key} label={`${label} (${matching.length})`} label_class="rdb-shape__node-label--category">
          {matching.map(source => (
            <ShapeNode key={source.name} label={source.name}>
              {source.columns.length > 0
                ? source.columns.map(col => (
                  <ShapeNode
                    key={col.name}
                    label={col.name}
                    label_class="rdb-shape__node-label--column"
                    type={col.type}
                    type_class={type_color_class(col.type)}
                  />
                ))
                : undefined}
            </ShapeNode>
          ))}
        </ShapeNode>
      );
    });

  const render_tree = (nodes: NamespaceTree[]) =>
    nodes.map(ns => (
      <ShapeNode key={ns.id} label={ns.local_name} label_class="rdb-shape__node-label--namespace">
        {render_sources(ns.sources)}
        {render_tree(ns.children)}
      </ShapeNode>
    ));

  return (
    <>
      {toolbar}
      <div>{render_tree(roots)}</div>
    </>
  );
}
