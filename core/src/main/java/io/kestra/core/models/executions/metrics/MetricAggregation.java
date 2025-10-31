package io.kestra.core.models.executions.metrics;

import lombok.experimental.SuperBuilder;

import java.time.Instant;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
public class MetricAggregation extends AbstractMetricAggregation {
    @NotNull
    public Instant date;
}
