package io.kestra.plugin.googleworkspace.calendar;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.EventAttendee;
import com.google.api.services.calendar.model.EventDateTime;
import com.google.api.services.calendar.model.Event.Creator;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractInsertEvent extends AbstractCalendar {

    @Schema(
        title = "Calendar ID"
    )
    @NotNull
    protected Property<String> calendarId;

    @Schema(
        title = "Title of the event"
    )
    @NotNull
    protected Property<String> summary;

    @Schema(
        title = "Description of the event"
    )
    @PluginProperty(dynamic = true)
    protected String description;

    @Schema(
        title = "Geographic location of the event as free-form text"
    )
    protected Property<String> location;

    @Schema(
        title = "Start time of the event"
    )
    @NotNull
    @PluginProperty
    protected CalendarTime startTime;

    @Schema(
        title = "End time of the event"
    )
    @NotNull
    @PluginProperty
    protected CalendarTime endTime;

    @Schema(
        title = "Creator of the event"
    )
    @PluginProperty
    protected Attendee creator;

    @Schema(
        title = "List of attendees in the event"
    )
    @PluginProperty
    protected List<Attendee> attendees;

    @Builder
    @ToString
    @EqualsAndHashCode
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CalendarTime {
        @Schema(
            title = "Time of the event in the ISO 8601 Datetime format, for example, `2024-11-28T09:00:00-07:00`"
        )
        protected Property<String> dateTime;

        @Schema(
            title = "Timezone associated with the dateTime, for example, `America/Los_Angeles`"
        )
        protected Property<String> timeZone;
    }

    @Builder
    @ToString
    @EqualsAndHashCode
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Attendee {
        @Schema(
            title = "Display name of the attendee"
        )
        protected Property<String> displayName;

        @Schema(
            title = "Email of the attendee"
        )
        protected Property<String> email;
    }

    protected Event event(RunContext runContext) throws IllegalVariableEvaluationException {
        Event eventMetadata = new Event();

        eventMetadata.setSummary(runContext.render(this.summary).as(String.class).orElseThrow());
        if (this.description != null) {
            eventMetadata.setDescription(runContext.render(this.description));
        }

        if (this.location != null) {
            eventMetadata.setLocation(runContext.render(this.location).as(String.class).orElseThrow());
        }

        EventDateTime eventStartTime = new EventDateTime().setDateTime(new DateTime(runContext.render(startTime.dateTime).as(String.class).orElse(null)))
            .setTimeZone(runContext.render(startTime.timeZone).as(String.class).orElse(null));
        eventMetadata.setStart(eventStartTime);

        EventDateTime eventEndTime = new EventDateTime().setDateTime(new DateTime(runContext.render(endTime.dateTime).as(String.class).orElse(null)))
            .setTimeZone(runContext.render(endTime.timeZone).as(String.class).orElse(null));
        eventMetadata.setEnd(eventEndTime);

        if (attendees != null && attendees.size() > 0) {
            List<EventAttendee> eventAttendees = new ArrayList<>();
            for (Attendee attendee: attendees){
                EventAttendee eventAttendee = new EventAttendee().setDisplayName(runContext.render(attendee.displayName).as(String.class).orElse(null))
                    .setEmail(runContext.render(attendee.email).as(String.class).orElse(null));
                eventAttendees.add(eventAttendee);
            }
            eventMetadata.setAttendees(eventAttendees);
        }

        if (creator != null) {
            Creator eventCreator = new Creator().setDisplayName(runContext.render(creator.displayName).as(String.class).orElse(null))
                .setEmail(runContext.render(creator.email).as(String.class).orElse(null));
            eventMetadata.setCreator(eventCreator);
        }

        return eventMetadata;
    }
}
