package io.kestra.worker.group.services;

import io.kestra.core.models.flows.FlowInterface;
import io.kestra.core.models.tasks.WorkerGroup;
import io.kestra.core.runners.WorkerJob;
import io.kestra.core.runners.WorkerTask;
import io.kestra.core.runners.WorkerTrigger;
import io.kestra.core.services.WorkerGroupService;
import io.micronaut.context.annotation.Primary;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Singleton
@Primary
@Slf4j
public class DefaultWorkerGroupService extends WorkerGroupService {
    public String resolveGroupFromKey(String workerGroupKey) {
        return workerGroupKey;
    }

    public Optional<WorkerGroup> resolveGroupFromJob(FlowInterface flow, WorkerJob workerJob) {
        WorkerGroup workerGroup = switch (workerJob) {
            case WorkerTask task -> task.getTask().getWorkerGroup();
            case WorkerTrigger trigger -> trigger.getTrigger().getWorkerGroup();
            default -> throw new IllegalArgumentException("Unknown worker job type '" + workerJob.getClass().getName() + "'");
        };
        return Optional.ofNullable(workerGroup).or(() -> Optional.ofNullable(flow.getWorkerGroup()));
    }
}
