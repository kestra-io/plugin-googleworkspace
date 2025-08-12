package io.kestra.plugin.googleworkspace.calendar;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
            title = "Delete a calendar Event",
            full = true,
            code = """
                id: googleworkspace_calendar_delete_event
                namespace: company.team

                tasks:
                  - id: cancel_event
                    type: io.kestra.plugin.googleworkspace.calendar.DeleteEvent
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                    calendarId: primary
                    eventId: "abcdef123456"
                    sendUpdates: all
                """
        )
    }
)
@Schema(title = "Delete a Google Calendar event.")
public class DeleteEvent extends AbstractCalendar implements RunnableTask<VoidOutput> {
    @Schema(title = "Calendar ID.")
    @NotNull
    protected Property<String> calendarId;

    @Schema(title = "Event ID.")
    @NotNull
    protected Property<String> eventId;

    @Schema(
        title = "Send update emails (default: none)",
        allowableValues = {"all", "none", "externalOnly"}
    )
    @Builder.Default
    protected Property<String> sendUpdates = Property.ofValue("none");

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        Calendar service = this.connection(runContext);

        String rCalendarId = runContext.render(calendarId).as(String.class).orElseThrow();
        String rEventId = runContext.render(eventId).as(String.class).orElseThrow();
        String rSendUpdates = runContext.render(sendUpdates).as(String.class).orElse("none");

        service.events()
                .delete(rCalendarId, rEventId)
                .setSendUpdates(rSendUpdates)
                .execute();

        return null;
    }
}