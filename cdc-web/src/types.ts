export type Locale = 'en' | 'zh'
export type View = 'overview' | 'tasks' | 'connections' | 'audit'
export type Connection = { id: string; name: string; type: string; options: Record<string, string>; configuredSecrets?: string[]; updated?: string | number }
export type Mapping = { rename: Record<string, string>; conversions: Record<string, string>; exclude: string[] }
export type Task = {
  id: string; name: string; sourceConnectionId: string; sinkConnectionId: string;
  snapshotMode: string; mapping: Mapping; state: string; error?: string; updated?: string | number;
  health?: { state: string; retries?: number; lastProgressMillis?: number; sourceTimeMillis?: number; errorCode?: string; journal?: { total: number; pending: number; bytes: number; acknowledged: number } }
}
export type Inspection = { identity: string; fields: Record<string, string>; keyFields: string[]; details: Record<string, unknown> }
export type Audit = { timestamp: string | number; action: string; resource: string; result: string }
export type ConnectorCaps = { sourceModes?: string[]; sinkModes?: string[]; types?: string[]; limits?: string[]; [key: string]: unknown }
export type ConnectionField = { key: string; label: string; placeholder?: string; secret?: boolean; choices?: string[]; full?: boolean; required?: boolean }
export type MappingRow = { source: string; target: string; conversion: string; excluded: boolean }
