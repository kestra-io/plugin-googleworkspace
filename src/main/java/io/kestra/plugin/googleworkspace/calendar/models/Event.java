package io.kestra.plugin.googleworkspace.calendar.models;

import lombok.Builder;
import lombok.Data;
import lombok.With;

@Data
@Builder
public class Event {
    @With
    private final String id;

    public static Event of(com.google.api.services.calendar.model.Event event) {
        return Event.builder()
            .id(event.getId())
            .build();
    }
}
