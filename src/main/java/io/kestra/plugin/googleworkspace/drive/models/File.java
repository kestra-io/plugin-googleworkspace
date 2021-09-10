package io.kestra.plugin.googleworkspace.drive.models;

import lombok.Builder;
import lombok.Data;
import lombok.With;

import java.time.Instant;
import java.util.List;

@Data
@Builder
public class File {
    @With
    private final String id;
    private final String name;
    private final Long size;
    private final Long version;
    private final String mimeType;
    private final Instant createdTime;
    private final List<String> parents;
    private final Boolean trashed;

    public static File of(com.google.api.services.drive.model.File file) {
        return File.builder()
            .id(file.getId())
            .name(file.getName())
            .size(file.getSize())
            .version(file.getVersion())
            .mimeType(file.getMimeType())
            .createdTime(file.getCreatedTime() != null ? Instant.parse(file.getCreatedTime().toStringRfc3339()) : null)
            .parents(file.getParents())
            .trashed(file.getTrashed())
            .build();
    }
}
