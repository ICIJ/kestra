package io.kestra.core.models.executions.metrics;

import jakarta.validation.constraints.NotNull;
import lombok.experimental.SuperBuilder;

@SuperBuilder()
public class AbstractMetricAggregation {
    @NotNull
    public String name;

    public Double value;
}
