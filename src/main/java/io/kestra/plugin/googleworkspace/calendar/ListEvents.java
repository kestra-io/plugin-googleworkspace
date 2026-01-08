package io.kestra.plugin.googleworkspace.calendar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Events;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;
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
            id: googleworkspace_calendar_list_events
            namespace: company.team

            tasks:
              - id: list_events
                type: io.kestra.plugin.googleworkspace.calendar.ListEvents
                serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                calendarId: team@company.com
                timeMin: "2025-08-10T00:00:00Z"
                timeMax: "2025-08-12T00:00:00Z"
                q: "standup"
                singleEvents: true
                orderBy: startTime
                showDeleted: false
                maxResults: 100
        """
    )
)
@Schema(
    title = "List Google Calendar events using filters (range, keyword, etc.)."
)
public class ListEvents extends AbstractCalendar implements RunnableTask<ListEvents.Output> {
    @Schema(title = "Calendar ID (e.g., team@company.com).")
    @NotNull
    protected Property<String> calendarId;

    @Schema(title = "Lower bound for an event's start time (RFC3339)")
    protected Property<String> timeMin;

    @Schema(title = "Upper bound for an event's end time (RFC3339)")
    protected Property<String> timeMax;

    @Schema(title = "Free-text search across title/description/location")
    protected Property<String> q;

    @Schema(title = "Return single instances of recurring events")
    @Builder.Default
    protected Property<Boolean> singleEvents = Property.ofValue(true);

    @Schema(title = "Order by 'startTime' (requires singleEvents=true) or 'updated'")
    protected Property<String> orderBy;

    @Schema(title = "Include cancelled events")
    @Builder.Default
    protected Property<Boolean> showDeleted = Property.ofValue(false);

    @Schema(title = "Max results (1–2500)")
    protected Property<Integer> maxResults;

    @Schema(title = "Page token for pagination")
    protected Property<String> pageToken;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Calendar service = this.connection(runContext);

        String rCalendarId = runContext.render(calendarId).as(String.class).orElseThrow();
        String rTimeMin = runContext.render(timeMin).as(String.class).orElse(null);
        String rTimeMax = runContext.render(timeMax).as(String.class).orElse(null);
        String rQuery = runContext.render(q).as(String.class).orElse(null);
        Boolean rSingle = runContext.render(singleEvents).as(Boolean.class).orElse(true);
        String rOrderBy = runContext.render(orderBy).as(String.class).orElse(null);
        Boolean rShowDeleted = runContext.render(showDeleted).as(Boolean.class).orElse(false);
        Integer rMaxResults = runContext.render(maxResults).as(Integer.class).orElse(null);
        String rPageToken = runContext.render(pageToken).as(String.class).orElse(null);

        var req = service.events().list(rCalendarId);

        if (rTimeMin != null) {
            req.setTimeMin(new DateTime(rTimeMin));
        }

        if (rTimeMax != null) {
            req.setTimeMax(new DateTime(rTimeMax));
        }
        if (rQuery   != null) {
            req.setQ(rQuery);
        }

        req.setSingleEvents(rSingle);

        if (rOrderBy != null) {
            req.setOrderBy(rOrderBy);
        }

        req.setShowDeleted(rShowDeleted);

        if (rMaxResults  != null) {
            req.setMaxResults(rMaxResults);
        }

        if (rPageToken   != null) {
            req.setPageToken(rPageToken);
        }

        Events rResp = req.execute();

        List<io.kestra.plugin.googleworkspace.calendar.models.Event> rEvents =
            rResp.getItems() == null
                ? List.of()
                : rResp.getItems().stream()
                .map(io.kestra.plugin.googleworkspace.calendar.models.Event::of)
                .toList();

        List<Map<String, Object>> rMetadataList =
            rResp.getItems() == null
                ? List.of()
                : rResp.getItems().stream()
                .map(ev -> JacksonMapper.ofJson().convertValue(ev, new TypeReference<Map<String, Object>>() {}))
                .toList();

        return Output.builder()
            .events(rEvents)
            .metadataList(rMetadataList)
            .nextPageToken(rResp.getNextPageToken())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Matched events (typed, convenient)")
        private final List<io.kestra.plugin.googleworkspace.calendar.models.Event> events;

        @Schema(title = "Full Google events (raw metadata, 1:1)")
        private final List<Map<String, Object>> metadataList;

        @Schema(title = "Pagination token for fetching the next page, if any")
        private final String nextPageToken;
    }
}