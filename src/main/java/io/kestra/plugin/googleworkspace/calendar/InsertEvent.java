package io.kestra.plugin.googleworkspace.calendar;

import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import org.slf4j.Logger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: googleworkspace_calendar_insert_event
                namespace: company.team

                tasks:
                  - id: insert_event
                    type: io.kestra.plugin.googleworkspace.calendar.InsertEvent
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                    calendarId: team@company.com
                    summary: Sample Event
                    description: This is a sample event from Kestra
                    location: Thane, Mumbai
                    startTime:
                      dateTime: "2024-11-28T09:00:00+05:30"
                      timeZone: "Asia/Calcutta"
                    endTime:
                      dateTime: "2024-11-28T10:00:00+05:30"
                      timeZone: "Asia/Calcutta"
                    creator:
                      email: myself@gmail.com
                """
        )
    }
)
@Schema(
    title = "Create a Google Calendar event",
    description = "Inserts a new event in the target calendar using a service account. Requires summary, start, and end times; optional location, description, creator, and attendees."
)
public class InsertEvent extends AbstractInsertEvent implements RunnableTask<InsertEvent.Output> {
    @Override
    public Output run(RunContext runContext) throws Exception {
        Calendar service = this.connection(runContext);
        Logger logger = runContext.logger();

        Event eventMetadata = event(runContext);

        var renderedCalendarId= runContext.render(calendarId).as(String.class).orElseThrow();
        Event event = service
            .events()
            .insert(renderedCalendarId, eventMetadata)
            .setFields("id")
            .execute();

        logger.debug("Inserted event '{}' in calendar '{}'", event.getId(), renderedCalendarId);

        return Output
            .builder()
            .event(io.kestra.plugin.googleworkspace.calendar.models.Event.of(event))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Inserted event",
            description = "Event resource containing the new event ID"
        )
        private final io.kestra.plugin.googleworkspace.calendar.models.Event event;
    }
}
