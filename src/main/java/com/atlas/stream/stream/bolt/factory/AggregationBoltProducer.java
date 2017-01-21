package com.atlas.stream.stream.bolt.factory;

import com.atlas.stream.stream.bolt.AggregationBolt;
import com.atlas.stream.stream.domain.AggregationLimits;

public class AggregationBoltProducer extends BoltProducer<AggregationBolt> {

    private final AggregationLimits aggregationLimits;

    public AggregationBoltProducer(AggregationLimits aggregationLimits) {
        super(AggregationBolt.class);
        this.aggregationLimits = aggregationLimits;
    }

    @Override
    public AggregationBolt create() {
         return new AggregationBolt(aggregationLimits);
    }
}
