import {useQuery} from "@/hooks/use-query.ts";
import {Schema} from '@reifydb/core';

export interface VersionRow {
    name: string;
    type: string;
    version: string;
    description: string;
}

export interface Version {
    version: string;
    modules: VersionRow[];
    subsystems: VersionRow[];
    builds: VersionRow[];
}

// Define the schema for the system.versions table
const versionSchema = Schema.object({
    name: Schema.string(),
    type: Schema.string(),
    version: Schema.string(),
    description: Schema.string()
});

export function useVersion(): [boolean, Version, string | undefined] {
    const {isExecuting, result, error} = useQuery<VersionRow>("FROM system.versions", versionSchema);

    // Extract values from Value objects if present
    const rows: VersionRow[] = result?.rows?.map(row => {
        // Check if we're dealing with Value objects or plain objects
        const isValueObject = row.name && typeof row.name === 'object' && 'valueOf' in row.name;
        
        if (isValueObject) {
            // Convert Value objects to plain values
            return {
                name: row.name?.valueOf?.() || '',
                type: row.type?.valueOf?.() || '',
                version: row.version?.valueOf?.() || '',
                description: row.description?.valueOf?.() || ''
            };
        }
        // Already plain objects from schema
        return row as VersionRow;
    }) || [];

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