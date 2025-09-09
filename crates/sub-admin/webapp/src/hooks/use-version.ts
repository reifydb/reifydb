import {useQueryOne} from "@/hooks/use-query.ts";
import {InferSchema, Schema} from '@reifydb/core';

export interface Version {
    version: string;
    modules: VersionRow[];
    subsystems: VersionRow[];
    builds: VersionRow[];
}

const versionSchema = Schema.object({
    name: Schema.string(),
    type: Schema.string(),
    version: Schema.string(),
    description: Schema.string()
});

type VersionRow = InferSchema<typeof versionSchema>;

export function useVersion(): [boolean, Version, string | undefined] {
    const {isExecuting, result, error} = useQueryOne("FROM system.versions", null, versionSchema);

    const rows: VersionRow[] = result?.rows || [];

    // Separate rows by type, apply transformations, and sort by name ascending
    const modules = rows.filter(r => r.type === 'module').sort((a, b) => a.name.localeCompare(b.name));
    const subsystems = rows.filter(r => r.type === 'subsystem').sort((a, b) => a.name.localeCompare(b.name));
    const builds = rows
        .filter(r => r.type === 'build')
        .map(r => ({...r, version: r.version.length > 7 ? r.version.substring(0, 7) : r.version}))
        .sort((a, b) => a.name.localeCompare(b.name));

    return [isExecuting, {
        version: rows.find(r => r.type === 'package')?.version || "N/A",
        modules,
        subsystems,
        builds
    }, error];
}