package io.kestra.plugin.googleworkspace.calendar;

import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.EventAttendee;
import com.google.api.services.calendar.model.EventDateTime;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = @Example(
        full = true,
        code = """
          id: googleworkspace_calendar_update_event
          namespace: company.team

          tasks:
            - id: update_event
              type: io.kestra.plugin.googleworkspace.calendar.UpdateEvent
              serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
              calendarId: team@company.com
              eventId: "abcdef123456"
              patch: true
              sendUpdates: externalOnly
              summary: "Weekly standup (rescheduled)"
              startTime:
                dateTime: "2025-08-12T10:00:00+05:30"
                timeZone: "Asia/Kolkata"
              endTime:
                dateTime: "2025-08-12T10:30:00+05:30"
                timeZone: "Asia/Kolkata"
              attendees:
                - email: a@example.com
                - email: team@example.com
        """
    )
)
@Schema(
    title = "Update a Google Calendar event",
    description = "Updates an existing event using a service account. Uses PATCH by default to change only provided fields; set `patch` to false for full replace. `sendUpdates` controls attendee emails (none by default)."
)
public class UpdateEvent extends AbstractCalendar implements RunnableTask<UpdateEvent.Output> {
    @Schema(
        title = "Calendar ID",
        description = "Email-style calendar shared with the service account"
    )
    @NotNull
    protected Property<String> calendarId;

    @Schema(
        title = "Event ID",
        description = "Identifier of the event to update"
    )
    @NotNull
    protected Property<String> eventId;

    @Schema(
        title = "Patch update",
        description = "When true (default) sends PATCH; false sends full UPDATE"
    )
    @Builder.Default
    protected Property<Boolean> patch = Property.ofValue(true);

    @Schema(
        title = "Send update emails",
        description = "Controls attendee notification: all, externalOnly, or none (default)"
    )
    @Builder.Default
    protected Property<String> sendUpdates = Property.ofValue("none");

    @Schema(
        title = "Title",
        description = "New event title (optional)"
    )
    protected Property<String> summary;

    @Schema(
        title = "Description",
        description = "New description; supports templating"
    )
    @io.kestra.core.models.annotations.PluginProperty(dynamic = true)
    protected String description;

    @Schema(
        title = "Location",
        description = "Free-form location to set"
    )
    protected Property<String> location;

    @Schema(
        title = "New start time",
        description = "Replacement start datetime/timezone"
    )
    @io.kestra.core.models.annotations.PluginProperty
    protected AbstractInsertEvent.CalendarTime startTime;

    @Schema(
        title = "New end time",
        description = "Replacement end datetime/timezone"
    )
    @io.kestra.core.models.annotations.PluginProperty
    protected AbstractInsertEvent.CalendarTime endTime;

    @Schema(
        title = "Attendees",
        description = "Replaces the attendee list with the provided entries"
    )
    @io.kestra.core.models.annotations.PluginProperty
    protected List<AbstractInsertEvent.Attendee> attendees;

    @Schema(
        title = "Event status",
        description = "Status value: confirmed, tentative, or cancelled"
    )
    protected Property<String> status;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Calendar service = this.connection(runContext);
        Logger logger = runContext.logger();

        String rCalendarId = runContext.render(calendarId).as(String.class).orElseThrow();
        String rEventId = runContext.render(eventId).as(String.class).orElseThrow();
        Boolean rPatch = runContext.render(patch).as(Boolean.class).orElse(true);
        String rSendUpdates = runContext.render(sendUpdates).as(String.class).orElse("none");
        String rSummary = runContext.render(summary).as(String.class).orElse(null);
        String rDescription = (description != null) ? runContext.render(description) : null;
        String rLocation = runContext.render(location).as(String.class).orElse(null);
        String rStartDateTime = (startTime != null) ? runContext.render(startTime.getDateTime()).as(String.class).orElse(null) : null;
        String rStartTimeZone = (startTime != null) ? runContext.render(startTime.getTimeZone()).as(String.class).orElse(null) : null;
        String rEndDateTime = (endTime != null) ? runContext.render(endTime.getDateTime()).as(String.class).orElse(null) : null;
        String rEndTimeZone = (endTime != null) ? runContext.render(endTime.getTimeZone()).as(String.class).orElse(null) : null;

        List<EventAttendee> rAttendees = null;
        if (attendees != null) {
            rAttendees = new ArrayList<>();
            for (var a : attendees) {
                String rDisplayName = runContext.render(a.getDisplayName()).as(String.class).orElse(null);
                String rEmail       = runContext.render(a.getEmail()).as(String.class).orElse(null);
                rAttendees.add(new EventAttendee().setDisplayName(rDisplayName).setEmail(rEmail));
            }
        }

        String rStatus = runContext.render(status).as(String.class).orElse(null);

        Event rBody = new Event();

        if (rSummary != null) {
            rBody.setSummary(rSummary);
        }

        if (rDescription != null) {
            rBody.setDescription(rDescription);
        }

        if (rLocation != null) {
            rBody.setLocation(rLocation);
        }

        if (rStartDateTime != null || rStartTimeZone != null) {
            rBody.setStart(new EventDateTime()
                .setDateTime(rStartDateTime != null ? new DateTime(rStartDateTime) : null)
                .setTimeZone(rStartTimeZone));

        }

        if (rEndDateTime != null || rEndTimeZone != null) {
            rBody.setEnd(new EventDateTime()
                .setDateTime(rEndDateTime != null ? new DateTime(rEndDateTime) : null)
                .setTimeZone(rEndTimeZone));
        }

        if (rAttendees != null) {
            rBody.setAttendees(rAttendees);
        }

        if (rStatus != null) {
            rBody.setStatus(rStatus);
        }

        Event updatedEvent = rPatch
            ? service.events().patch(rCalendarId, rEventId, rBody).setSendUpdates(rSendUpdates).execute()
            : service.events().update(rCalendarId, rEventId, rBody).setSendUpdates(rSendUpdates).execute();

        logger.debug("{} event '{}' in calendar '{}'", rPatch ? "Patched" : "Updated", rEventId, rCalendarId);

        return Output.builder()
            .event(io.kestra.plugin.googleworkspace.calendar.models.Event.of(updatedEvent))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Updated event")
        private final io.kestra.plugin.googleworkspace.calendar.models.Event event;
    }
}
