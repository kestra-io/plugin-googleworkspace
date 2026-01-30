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
        title = "Calendar ID",
        description = "Email-style calendar to create the event in; must be shared with the service account"
    )
    @NotNull
    protected Property<String> calendarId;

    @Schema(
        title = "Event title",
        description = "Required summary shown in the calendar"
    )
    @NotNull
    protected Property<String> summary;

    @Schema(
        title = "Event description",
        description = "Optional body text; supports templating"
    )
    @PluginProperty(dynamic = true)
    protected String description;

    @Schema(
        title = "Location",
        description = "Free-form place text such as city, room, or address"
    )
    protected Property<String> location;

    @Schema(
        title = "Start time",
        description = "RFC3339 datetime for the start; include offset"
    )
    @NotNull
    @PluginProperty
    protected CalendarTime startTime;

    @Schema(
        title = "End time",
        description = "RFC3339 datetime for the end; must be after start"
    )
    @NotNull
    @PluginProperty
    protected CalendarTime endTime;

    @Schema(
        title = "Creator",
        description = "Optional explicit creator shown on the event"
    )
    @PluginProperty
    protected Attendee creator;

    @Schema(
        title = "Attendees",
        description = "Replaces the attendee list on creation"
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
            title = "Datetime",
            description = "RFC3339 timestamp with offset, e.g. 2024-11-28T09:00:00-07:00"
        )
        protected Property<String> dateTime;

        @Schema(
            title = "Timezone",
            description = "IANA timezone, e.g. America/Los_Angeles"
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
            title = "Attendee name",
            description = "Optional display name"
        )
        protected Property<String> displayName;

        @Schema(
            title = "Attendee email",
            description = "Email address of the attendee"
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
