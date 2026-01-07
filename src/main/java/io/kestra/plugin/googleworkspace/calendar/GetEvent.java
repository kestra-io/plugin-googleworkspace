package io.kestra.plugin.googleworkspace.calendar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = @Example(
        full = true,
        code = """
            id: googleworkspace_calendar_get_event
            namespace: company.team

            tasks:
              - id: get_event
                type: io.kestra.plugin.googleworkspace.calendar.GetEvent
                serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                calendarId: team@company.com
                eventId: "abcdef123456"
                maxAttendees: 50
                alwaysIncludeEmail: true
            """
    )
)
@Schema(title = "Fetch a Google Calendar event by ID.")
public class GetEvent extends AbstractCalendar implements RunnableTask<GetEvent.Output> {
    @Schema(title = "Calendar ID (e.g., a calendar email)")
    @NotNull
    protected Property<String> calendarId;

    @Schema(title = "Event ID")
    @NotNull
    protected Property<String> eventId;

    @Schema(title = "Upper bound on the number of attendees to include")
    protected Property<Integer> maxAttendees;

    @Schema(title = "Whether to include the email of the organizer/attendees in the response")
    @Builder.Default
    protected Property<Boolean> alwaysIncludeEmail = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Calendar service = this.connection(runContext);
        Logger logger = runContext.logger();

        String rCalendarId = runContext.render(calendarId).as(String.class).orElseThrow();
        String rEventId = runContext.render(eventId).as(String.class).orElseThrow();
        Integer rMaxAttendees = runContext.render(maxAttendees).as(Integer.class).orElse(null);
        Boolean rAlwaysIncludeEmail= runContext.render(alwaysIncludeEmail).as(Boolean.class).orElse(false);

        var req = service.events().get(rCalendarId, rEventId);
        if (rMaxAttendees != null) req.setMaxAttendees(rMaxAttendees);
        req.setAlwaysIncludeEmail(rAlwaysIncludeEmail);

        Event googleEvent = req.execute();

        Map<String, Object> eventMetadata = JacksonMapper.ofJson().convertValue(
            googleEvent, new TypeReference<Map<String, Object>>() {}
        );

        logger.debug("fetched event '{}' from calendar '{}'", rEventId, rCalendarId);

        return Output.builder()
            .event(io.kestra.plugin.googleworkspace.calendar.models.Event.of(googleEvent))
            .metadata(eventMetadata)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Full Google Calendar event resource (wrapped)")
        private final io.kestra.plugin.googleworkspace.calendar.models.Event event;

        @Schema(title = "Complete metadata of Google Calendar event resource")
        private final Map<String, Object> metadata;
    }
}