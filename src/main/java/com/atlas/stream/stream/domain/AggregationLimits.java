package com.atlas.stream.stream.domain;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import lombok.Data;

@Data
public class AggregationLimits implements Serializable {

    private final long periodMillis;
    private final int count;

    @JsonCreator
    public AggregationLimits(int timeMins, int count) {
        this.periodMillis = TimeUnit.MINUTES.toMillis(timeMins);
        this.count = count;
    }


}
