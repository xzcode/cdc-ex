package com.cdcex.core;

import java.nio.file.Path;
import java.util.*;

/** Connector-facing management SPI; it never exposes driver-specific classes. */
public interface ConnectorProvider {
    enum Role { SOURCE, SINK }
    record Inspection(String identity, Map<String,String> fields, List<String> keyFields, Map<String,Object> details) {
        public Inspection {
            Objects.requireNonNull(identity);
            fields=Collections.unmodifiableMap(new LinkedHashMap<>(fields));
            keyFields=List.copyOf(keyFields);
            details=Collections.unmodifiableMap(new LinkedHashMap<>(details));
        }
        public String schemaFingerprint() { return EventCodec.digest(List.of(fields,keyFields)); }
    }
    record SourceContext(Path directory, String taskId, String snapshotMode, boolean fresh) {}
    record SinkContext(Path directory, String taskId, boolean fresh) {
        public SinkContext(Path directory,String taskId){this(directory,taskId,true);}
    }
    String type();
    Map<String,Object> capabilities();
    /** identity includes physical resource identity and namespace; details must contain no secrets. */
    Inspection inspect(Endpoint endpoint, Role role) throws Exception;
    SourceConnector openSource(Endpoint endpoint, SourceContext context) throws Exception;
    /** Receives events after mapping, addressed to this endpoint's namespace and keys. */
    SinkConnector openSink(Endpoint endpoint, SinkContext context) throws Exception;
    /** Bounded validation scan. Return at most limit rows; callers ask for limit+1 to detect truncation. */
    default List<Map<String,Object>> readRows(Endpoint endpoint, int limit) throws Exception {
        throw new UnsupportedOperationException("Comparison is not supported by this connector");
    }
}
