import type { ConnectionField } from './types'

export const connectorNames: Record<string, string> = { MYSQL: 'MySQL', MARIADB: 'MariaDB', MONGODB: 'MongoDB', POSTGRESQL: 'PostgreSQL', CLICKHOUSE: 'ClickHouse', KAFKA: 'Kafka' }
export const connectorMarks: Record<string, string> = { MYSQL: 'My', MARIADB: 'Ma', MONGODB: 'Mo', POSTGRESQL: 'Pg', CLICKHOUSE: 'Ch', KAFKA: 'K' }
const host: ConnectionField[] = [{ key: 'host', label: 'host', required: true, placeholder: 'db.example' }, { key: 'port', label: 'port', required: true }]
const auth: ConnectionField[] = [{ key: 'user', label: 'user' }, { key: 'password', label: 'password', secret: true }]
const namespace: ConnectionField[] = [{ key: 'database', label: 'database', required: true }, { key: 'table', label: 'table', required: true }]
export function connectionFields(type: string): ConnectionField[] {
  if (type === 'KAFKA') return [
    { key: 'bootstrapServers', label: 'bootstrapServers', required: true, full: true, placeholder: 'broker.example:9093' },
    { key: 'topic', label: 'topic', required: true }, { key: 'groupId', label: 'groupId' },
    { key: 'securityProtocol', label: 'securityProtocol', choices: ['SSL', 'SASL_SSL', 'PLAINTEXT'] },
    { key: 'startOffset', label: 'startOffset', choices: ['earliest', 'latest'] },
    { key: 'saslMechanism', label: 'saslMechanism', choices: ['SCRAM-SHA-512', 'SCRAM-SHA-256', 'PLAIN'] },
    { key: 'saslUsername', label: 'saslUsername' }, { key: 'saslPassword', label: 'saslPassword', secret: true },
    { key: 'sslTruststoreLocation', label: 'sslTruststoreLocation' }, { key: 'sslTruststorePassword', label: 'sslTruststorePassword', secret: true },
    { key: 'sslKeystoreLocation', label: 'sslKeystoreLocation' }, { key: 'sslKeystorePassword', label: 'sslKeystorePassword', secret: true },
    { key: 'sslKeyPassword', label: 'sslKeyPassword', secret: true },
  ]
  if (type === 'MONGODB') return [...host, ...namespace, ...auth,
    { key: 'replicaSet', label: 'replicaSet' }, { key: 'authDatabase', label: 'authDatabase' },
    { key: 'documentMode', label: 'documentMode', choices: ['replace'] }, { key: 'uri', label: 'uri', secret: true, full: true },
  ]
  if (type === 'POSTGRESQL') return [...host, ...namespace, { key: 'schema', label: 'schema' }, ...auth,
    { key: 'sslMode', label: 'sslMode', choices: ['verify-full', 'verify-ca', 'require', 'disable'] },
    { key: 'slot', label: 'slot' }, { key: 'publication', label: 'publication' }, { key: 'sslRootCert', label: 'sslRootCert', full: true },
  ]
  if (type === 'CLICKHOUSE') return [...host, ...namespace, ...auth,
    { key: 'mode', label: 'mode', choices: ['current_state', 'history', 'snapshot', 'cursor', 'changelog'] },
    { key: 'keyColumns', label: 'keyColumns', placeholder: 'id' }, { key: 'cursorColumns', label: 'cursorColumns', placeholder: 'seq,id' },
    { key: 'cursorGuarantee', label: 'cursorGuarantee', choices: ['', 'monotonic-visible', 'contiguous-visible'] },
    { key: 'startSequence', label: 'startSequence', placeholder: '0' },
  ]
  return [...host, ...namespace, ...auth,
    { key: 'sslMode', label: 'sslMode', choices: ['VERIFY_IDENTITY', 'VERIFY_CA', 'REQUIRED', 'DISABLED'] },
    { key: 'replicationServerId', label: 'replicationServerId', placeholder: '47123' },
  ]
}
export function defaultOptions(type: string): Record<string, string> {
  if (type === 'KAFKA') return { securityProtocol: 'SSL', startOffset: 'earliest', saslMechanism: 'SCRAM-SHA-512', format: 'CDC_EX' }
  if (type === 'MONGODB') return { port: '27017', tls: 'true', authDatabase: 'admin', documentMode: 'replace' }
  if (type === 'POSTGRESQL') return { port: '5432', schema: 'public', sslMode: 'verify-full' }
  if (type === 'CLICKHOUSE') return { user: 'default', port: '8443', tls: 'true', mode: 'current_state', exclusiveWriter: 'false' }
  return { port: '3306', sslMode: 'VERIFY_IDENTITY' }
}
export const conversions = ['STRING', 'LONG', 'INTEGER', 'DECIMAL', 'BOOLEAN', 'JSON', 'BSON_INT64', 'BSON_DECIMAL', 'BSON_OBJECT_ID']
export function isSecret(key: string) { return /password|secret|token|credential|^uri$/i.test(key) }
