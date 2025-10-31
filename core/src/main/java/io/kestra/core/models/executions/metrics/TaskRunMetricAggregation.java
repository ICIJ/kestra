package io.kestra.core.models.executions.metrics;

import jakarta.validation.constraints.NotNull;
import lombok.experimental.SuperBuilder;

@SuperBuilder()
public class TaskRunMetricAggregation extends AbstractMetricAggregation {
    @NotNull
    public String taskId;

    @NotNull
    public String taskRunId;
}
