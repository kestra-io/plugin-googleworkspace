package io.kestra.plugin.googleworkspace;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

public interface GcpInterface {
    @Schema(
        title = "The GCP service account key"
    )
    @PluginProperty(dynamic = true)
    String getServiceAccount();

    @Schema(
        title = "The GCP scopes to used"
    )
    @PluginProperty(dynamic = true)
    List<String> getScopes();
}
