package io.kestra.plugin.core.external;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;

public interface ExternalTaskInterface {
    @Schema(
        title = "name of the Python task"
    )
    Property<String> getName();

    @Schema(
        title = "task inputs"
    )
    Property<Map<String, Object>> getInputs();
}
