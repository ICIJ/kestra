package io.kestra.plugin.core.external;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Task run by a Python worker",
    description = """
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Executes a task inside a Python worker",
            full = true,
            code = """
                id: hello_world
                namespace: my.company

                inputs:
                  - id: greeted
                    type: STRING
                    displayName: "Person to greet"

                tasks:
                  - id: hello
                    type: io.kestra.plugin.core.external.Task
                    name: hello_world
                    inputs: "{{ inputs }}"
                """
        ),
    }
)
public class Task extends io.kestra.core.models.tasks.Task implements ExternalTaskInterface, RunnableTask<Task.Output> {
    @NotNull
    protected Property<String> name;

    @NotNull
    protected Property<Map<String, Object>> inputs;

    public Output run(RunContext runContext) throws Exception {
        // This is a placeholder
        return new Output();
    }

    @Builder(toBuilder = true)
    @Getter
    public static class Output extends HashMap<String, Object> implements io.kestra.core.models.tasks.Output {
    }
}
