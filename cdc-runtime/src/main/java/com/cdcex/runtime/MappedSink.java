package com.cdcex.runtime;

import com.cdcex.core.*;
import java.util.*;

public final class MappedSink implements SequencedSinkConnector {
    private final SinkConnector delegate;
    private final MappingSpec mapping;
    private final Endpoint endpoint;
    public MappedSink(SinkConnector delegate,MappingSpec mapping,Endpoint endpoint){this.delegate=delegate;this.mapping=mapping;this.endpoint=endpoint;}
    @Override public void applySequenced(List<Delivery> entries)throws Exception{
        List<Delivery> mapped=entries.stream().map(e->new Delivery(e.sequence(),mapping.apply(e.event(),endpoint))).toList();
        if(delegate instanceof SequencedSinkConnector s)s.applySequenced(mapped);
        else delegate.apply(mapped.stream().map(Delivery::event).toList());
    }
    @Override public void close()throws Exception{delegate.close();}
}
