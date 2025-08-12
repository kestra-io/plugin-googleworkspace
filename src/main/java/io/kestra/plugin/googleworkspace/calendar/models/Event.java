package io.kestra.plugin.googleworkspace.calendar.models;

import com.google.api.services.calendar.model.EventDateTime;
import lombok.Builder;
import lombok.Data;
import lombok.With;

@Data
@Builder
public class Event {
    @With
    private final String id;
    @With
    private final String status;
    @With
    private final String summary;
    @With
    private final String description;
    @With
    private final String location;

    public static Event of(com.google.api.services.calendar.model.Event event) {
        if (event == null) return null;
        return Event.builder()
            .id(event.getId())
            .status(event.getStatus())
            .summary(event.getSummary())
            .description(event.getDescription())
            .location(event.getLocation())
            .build();
    }
}
