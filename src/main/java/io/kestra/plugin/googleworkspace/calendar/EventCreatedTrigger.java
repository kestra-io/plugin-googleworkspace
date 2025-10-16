package io.kestra.plugin.googleworkspace.calendar;

import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.Events;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger that listens for new events created in a Google Calendar",
    description = "Monitors specified calendar(s) for newly created events and emits an event for each new event detected. " +
        "The trigger uses polling to check for new events based on their creation time."
)
@Plugin(
    examples = {
        @Example(
            title = "Monitor primary calendar for any new event",
            full = true,
            code = """
                id: google_calendar_event_trigger
                namespace: company.team

                tasks:
                  - id: process_event
                    type: io.kestra.plugin.core.log.Log
                    message: "New event created: {{ trigger.summary }} ({{ trigger.id }})"

                triggers:
                  - id: watch_calendar
                    type: io.kestra.plugin.googleworkspace.calendar.EventCreatedTrigger
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                    calendarId: primary
                    interval: PT5M
                """
        ),
        @Example(
            title = "Monitor specific calendar with keyword filter",
            full = true,
            code = """
                id: google_calendar_meeting_trigger
                namespace: company.team

                tasks:
                  - id: notify_meeting
                    type: io.kestra.plugin.notifications.slack.SlackIncomingWebhook
                    url: "{{ secret('SLACK_WEBHOOK') }}"
                    payload: |
                      {
                        "text": "New meeting scheduled: {{ trigger.summary }} on {{ trigger.start.dateTime }}"
                      }

                triggers:
                  - id: watch_meetings
                    type: io.kestra.plugin.googleworkspace.calendar.EventCreatedTrigger
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                    calendarId: "team-calendar@company.com"
                    q: "meeting"
                    interval: PT10M
                """
        ),
        @Example(
            title = "Monitor multiple calendars with organizer filter",
            full = true,
            code = """
                id: google_calendar_multiple_trigger
                namespace: company.team

                tasks:
                  - id: log_event
                    type: io.kestra.plugin.core.log.Log
                    message: "Event by {{ trigger.organizer.email }}: {{ trigger.summary }}"

                triggers:
                  - id: watch_multiple_calendars
                    type: io.kestra.plugin.googleworkspace.calendar.EventCreatedTrigger
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                    calendarIds:
                      - primary
                      - "team-calendar@company.com"
                      - "project-calendar@company.com"
                    organizerEmail: "manager@company.com"
                    interval: PT15M
                """
        )
    }
)
public class EventCreatedTrigger extends AbstractCalendarTrigger implements PollingTriggerInterface, TriggerOutput<EventCreatedTrigger.EventMetadata> {

    @Schema(
        title = "The Google Cloud service account key",
        description = "Service account JSON key with access to Google Calendar API"
    )
    @NotNull
    protected Property<String> serviceAccount;

    @Schema(
        title = "Calendar ID to monitor",
        description = "Calendar ID (e.g., 'primary' or a calendar email). Use this OR calendarIds, not both."
    )
    protected Property<String> calendarId;

    @Schema(
        title = "Multiple Calendar IDs to monitor",
        description = "List of calendar IDs to monitor. Use this OR calendarId, not both."
    )
    protected Property<List<String>> calendarIds;

    @Schema(
        title = "Free-text search filter",
        description = "Only events matching this search term will trigger. Searches across title, description, and location."
    )
    protected Property<String> q;

    @Schema(
        title = "Organizer email filter",
        description = "Only events organized by this email address will trigger events"
    )
    protected Property<String> organizerEmail;

    @Schema(
        title = "Event status filter",
        description = "Only events with this status will trigger. Options: confirmed, tentative, cancelled"
    )
    protected Property<String> eventStatus;

    @Schema(
        title = "The polling interval",
        description = "How frequently to check for new events. Must be at least PT1M (1 minute)."
    )
    @Builder.Default
    protected Duration interval = Duration.ofMinutes(5);

    @Schema(
        title = "Maximum number of events to process per poll",
        description = "Limits the number of new events processed in a single poll to avoid overwhelming the system"
    )
    @Builder.Default
    protected Property<Integer> maxEventsPerPoll = Property.ofValue(100);

    @Override
    public Duration getInterval() {
        return this.interval;
    }

    @Override
    public Property<Integer> getReadTimeout() {
        return Property.ofValue(120);
    }

    @Override
    public Property<List<String>> getScopes() {
        return Property.ofValue(List.of("https://www.googleapis.com/auth/calendar"));
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        validateConfiguration(runContext);

        // Create Calendar connection
        Calendar calendarService = this.connection(runContext);

        // Find the time window for checking new events
        Instant lastCreatedTime = context.getNextExecutionDate() != null
            ? context.getNextExecutionDate().toInstant().minus((TemporalAmount) this.interval)
            : Instant.now().minus((TemporalAmount) this.interval);

        logger.debug("Checking for events created after: {}", lastCreatedTime);

        List<String> calendarsToMonitor = getCalendarsToMonitor(runContext);
        
        List<EventMetadata> allNewEvents = new ArrayList<>();

        //Check every calendar for new events
        for (String calId : calendarsToMonitor) {
            try {
                List<EventMetadata> newEvents = checkCalendarForNewEvents(
                    calendarService, runContext, calId, lastCreatedTime, logger
                );
                allNewEvents.addAll(newEvents);
            } catch (Exception e) {
                logger.warn("Error checking calendar '{}': {}", calId, e.getMessage());
            }
        }

        if (allNewEvents.isEmpty()) {
            return Optional.empty();
        }

        logger.info("Found {} new event(s)", allNewEvents.size());

        // Create an execution with the first event as variables
        EventMetadata firstEvent = allNewEvents.get(0);
        
        // Build execution with event data as variables
        Execution execution = conditionContext.getExecution().toBuilder()
            .variables(Map.of(
                "eventId", firstEvent.getId(),
                "summary", firstEvent.getSummary() != null ? firstEvent.getSummary() : "",
                "description", firstEvent.getDescription() != null ? firstEvent.getDescription() : "",
                "location", firstEvent.getLocation() != null ? firstEvent.getLocation() : "",
                "status", firstEvent.getStatus() != null ? firstEvent.getStatus() : "",
                "htmlLink", firstEvent.getHtmlLink() != null ? firstEvent.getHtmlLink() : "",
                "organizer", firstEvent.getOrganizer() != null ? 
                    Map.of(
                        "email", firstEvent.getOrganizer().getEmail() != null ? firstEvent.getOrganizer().getEmail() : "",
                        "displayName", firstEvent.getOrganizer().getDisplayName() != null ? firstEvent.getOrganizer().getDisplayName() : ""
                    ) : Map.of(),
                "start", firstEvent.getStart() != null ?
                    Map.of(
                        "dateTime", firstEvent.getStart().getDateTime() != null ? firstEvent.getStart().getDateTime().toString() : "",
                        "timeZone", firstEvent.getStart().getTimeZone() != null ? firstEvent.getStart().getTimeZone() : ""
                    ) : Map.of(),
                "end", firstEvent.getEnd() != null ?
                    Map.of(
                        "dateTime", firstEvent.getEnd().getDateTime() != null ? firstEvent.getEnd().getDateTime().toString() : "",
                        "timeZone", firstEvent.getEnd().getTimeZone() != null ? firstEvent.getEnd().getTimeZone() : ""
                    ) : Map.of()
            ))
            .build();
            
        return Optional.of(execution);
    }

    private void validateConfiguration(RunContext runContext) throws Exception {
        // Make sure only calendarId or calendarIds is specified
        boolean hasCalendarId = this.calendarId != null && 
            runContext.render(this.calendarId).as(String.class).isPresent();
        boolean hasCalendarIds = this.calendarIds != null && 
            !runContext.render(this.calendarIds).asList(String.class).isEmpty();

        if (hasCalendarId && hasCalendarIds) {
            throw new IllegalArgumentException(
                "Cannot specify both 'calendarId' and 'calendarIds'. Use only one of them."
            );
        }

        // Validate interval minimum
        if (this.interval.toMillis() < Duration.ofMinutes(1).toMillis()) {
            throw new IllegalArgumentException(
                "Polling interval must be at least 1 minute (PT1M)"
            );
        }

        // Validate max events per poll
        Integer maxEvents = runContext.render(this.maxEventsPerPoll).as(Integer.class).orElse(100);
        if (maxEvents < 1 || maxEvents > 2500) {
            throw new IllegalArgumentException(
                "maxEventsPerPoll must be between 1 and 2500"
            );
        }
    }

    protected List<String> getCalendarsToMonitor(RunContext runContext) throws Exception {
        List<String> calendars = new ArrayList<>();
        
        if (this.calendarId != null) {
            String renderedCalendarId = runContext.render(this.calendarId).as(String.class).orElse(null);
            if (renderedCalendarId != null) {
                calendars.add(renderedCalendarId);
            }
        }
        
        if (this.calendarIds != null) {
            List<String> renderedCalendarIds = runContext.render(this.calendarIds).asList(String.class);
            calendars.addAll(renderedCalendarIds);
        }
        
        if (calendars.isEmpty()) {
            calendars.add("primary"); // Default to primary calendar
        }
        
        return calendars;
    }

    private List<EventMetadata> checkCalendarForNewEvents(
        Calendar calendarService, 
        RunContext runContext, 
        String calendarId, 
        Instant lastCreatedTime,
        Logger logger
    ) throws Exception {
        
        DateTime timeMin = new DateTime(lastCreatedTime.toEpochMilli());
        
        // Build the Calendar API request
        Calendar.Events.List request = calendarService.events().list(calendarId)
            .setTimeMin(timeMin)
            .setOrderBy("updated")
            .setSingleEvents(true)
            .setMaxResults(runContext.render(this.maxEventsPerPoll).as(Integer.class).orElse(100));

        // Add search filter if specified
        if (this.q != null) {
            String searchQuery = runContext.render(this.q).as(String.class).orElse(null);
            if (searchQuery != null && !searchQuery.trim().isEmpty()) {
                request.setQ(searchQuery);
            }
        }

        // Execute the request
        Events events = request.execute();
        List<Event> eventList = events.getItems();

        if (eventList == null || eventList.isEmpty()) {
            return Collections.emptyList();
        }

        logger.debug("Found {} events in calendar '{}', filtering for new ones...", eventList.size(), calendarId);

        return eventList.stream()
            .filter(event -> isNewEvent(event, lastCreatedTime, runContext))
            .map(this::convertToEventMetadata)
            .collect(Collectors.toList());
    }

    protected boolean isNewEvent(Event event, Instant lastCreatedTime, RunContext runContext) {
        try {
            // Check if event was created after our last check time
            if (event.getCreated() != null) {
                Instant eventCreatedTime = Instant.ofEpochMilli(event.getCreated().getValue());
                if (!eventCreatedTime.isAfter(lastCreatedTime)) {
                    return false;
                }
            }

            // Filter by organizer email if specified
            if (this.organizerEmail != null && event.getOrganizer() != null) {
                String filterEmail = runContext.render(this.organizerEmail).as(String.class).orElse(null);
                if (filterEmail != null && !filterEmail.equalsIgnoreCase(event.getOrganizer().getEmail())) {
                    return false;
                }
            }

            // Filter by event status if specified
            if (this.eventStatus != null) {
                String filterStatus = runContext.render(this.eventStatus).as(String.class).orElse(null);
                if (filterStatus != null && !filterStatus.equalsIgnoreCase(event.getStatus())) {
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
            // If there's any error evaluating the event, exclude it to be safe
            return false;
        }
    }

    protected EventMetadata convertToEventMetadata(Event event) {
        return EventMetadata.builder()
            .id(event.getId())
            .summary(event.getSummary())
            .description(event.getDescription())
            .location(event.getLocation())
            .status(event.getStatus())
            .htmlLink(event.getHtmlLink())
            .created(parseDateTime(event.getCreated()))
            .updated(parseDateTime(event.getUpdated()))
            .start(convertEventDateTime(event.getStart()))
            .end(convertEventDateTime(event.getEnd()))
            .organizer(convertOrganizer(event.getOrganizer()))
            .visibility(event.getVisibility())
            .eventType(event.getEventType())
            .build();
    }

    private ZonedDateTime parseDateTime(DateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        return ZonedDateTime.ofInstant(
            Instant.ofEpochMilli(dateTime.getValue()),
            ZoneId.systemDefault()
        );
    }

    private EventCreatedTrigger.EventDateTime convertEventDateTime(com.google.api.services.calendar.model.EventDateTime eventDateTime) {
        if (eventDateTime == null) {
            return null;
        }
        return EventCreatedTrigger.EventDateTime.builder()
            .dateTime(parseDateTime(eventDateTime.getDateTime()))
            .date(eventDateTime.getDate() != null ? eventDateTime.getDate().toString() : null)
            .timeZone(eventDateTime.getTimeZone())
            .build();
    }

    private EventCreatedTrigger.Organizer convertOrganizer(com.google.api.services.calendar.model.Event.Organizer organizer) {
        if (organizer == null) {
            return null;
        }
        return EventCreatedTrigger.Organizer.builder()
            .email(organizer.getEmail())
            .displayName(organizer.getDisplayName())
            .self(organizer.getSelf())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List of new events found during the poll",
            description = "All new events created since the last polling interval."
        )
        private List<EventMetadata> events;
    }

    @Builder
    @Getter
    public static class EventMetadata implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The event ID")
        private String id;

        @Schema(title = "The event title/summary")
        private String summary;

        @Schema(title = "The event description")
        private String description;

        @Schema(title = "The event location")
        private String location;

        @Schema(title = "The event status")
        private String status;

        @Schema(title = "Link to the event in Google Calendar")
        private String htmlLink;

        @Schema(title = "When the event was created")
        private ZonedDateTime created;

        @Schema(title = "When the event was last updated")
        private ZonedDateTime updated;

        @Schema(title = "The start time of the event")
        private EventCreatedTrigger.EventDateTime start;

        @Schema(title = "The end time of the event")
        private EventCreatedTrigger.EventDateTime end;

        @Schema(title = "The organizer of the event")
        private EventCreatedTrigger.Organizer organizer;

        @Schema(title = "Visibility of the event")
        private String visibility;

        @Schema(title = "Type of the event")
        private String eventType;
    }

    @Builder
    @Getter
    public static class EventDateTime {
        @Schema(title = "The date and time")
        private ZonedDateTime dateTime;

        @Schema(title = "The date (for all-day events)")
        private String date;

        @Schema(title = "The time zone")
        private String timeZone;
    }

    @Builder
    @Getter
    public static class Organizer {
        @Schema(title = "The organizer's email")
        private String email;

        @Schema(title = "The organizer's display name")
        private String displayName;

        @Schema(title = "Whether this is the current user")
        private Boolean self;
    }
}