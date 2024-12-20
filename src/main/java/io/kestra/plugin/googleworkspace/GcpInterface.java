package io.kestra.plugin.googleworkspace;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

public interface GcpInterface {
    @Schema(
        title = "The GCP service account key"
    )
    Property<String> getServiceAccount();

    @Schema(
        title = "The GCP scopes to used"
    )
    Property<List<String>> getScopes();

    @Schema(
        title = "The read timeout for the request (in seconds)"
    )
    Property<Integer> getReadTimeout();
}
