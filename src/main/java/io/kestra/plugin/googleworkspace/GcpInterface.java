package io.kestra.plugin.googleworkspace;

import java.util.List;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

public interface GcpInterface {
    @Schema(
        title = "The GCP service account key"
    )
    @PluginProperty(secret = true, group = "execution")
    Property<String> getServiceAccount();

    @Schema(
        title = "The GCP scopes to used"
    )
    @PluginProperty(group = "advanced")
    Property<List<String>> getScopes();

    @Schema(
        title = "The read timeout for the request (in seconds)"
    )
    @PluginProperty(group = "execution")
    Property<Integer> getReadTimeout();
}
