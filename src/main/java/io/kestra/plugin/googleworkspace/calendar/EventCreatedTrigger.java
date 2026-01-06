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
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.*;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger that listens for new events created in a Google Calendar",
    description = "Monitors one or multiple Google Calendars for newly created events and emits a Kestra execution when new events are detected. " +
        "The trigger polls calendars at regular intervals and detects events based on their creation time. " +
        "\n\n" +
        "Authentication: Requires a Google Cloud service account with Calendar API access (scope: `https://www.googleapis.com/auth/calendar`). " +
        "Share the target calendars with the service account email address. " +
        "\n\n" +
        "Configuration: Specify one or more calendar IDs in `calendarIds` (defaults to 'primary' if not specified). If not primary, the calendarId must be an email address like `team@company.com` or `your-name@company.com` depending on your organization." +
        "Set a polling `interval` (minimum PT1M). Optionally filter events by keywords (`searchQuery`), organizer email, or status. " +
        "\n\n" +
        "Performance: Each calendar requires a separate API call. Use filters to reduce processing load and set `maxEventsPerPoll` to avoid overwhelming the system. " +
        "The trigger automatically handles errors and continues monitoring if one calendar fails."
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
                  - id: process_events
                    type: io.kestra.plugin.core.log.Log
                    message: "Found {{ trigger.events | length }} new event(s)"
                  - id: log_each_event
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.events }}"
                    tasks:
                      - id: log_event
                        type: io.kestra.plugin.core.log.Log
                        message: "Event: {{ taskrun.value.summary }} ({{ taskrun.value.id }})"

                triggers:
                  - id: watch_calendar
                    type: io.kestra.plugin.googleworkspace.calendar.EventCreatedTrigger
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                    calendarIds:
                      - primary
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
                  - id: notify_meetings
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.events }}"
                    tasks:
                      - id: send_notification
                        type: io.kestra.plugin.notifications.slack.SlackIncomingWebhook
                        url: "{{ secret('SLACK_WEBHOOK') }}"
                        payload: |
                          {
                            "text": "New meeting scheduled: {{ taskrun.value.summary }} on {{ taskrun.value.start.dateTime }}"
                          }

                triggers:
                  - id: watch_meetings
                    type: io.kestra.plugin.googleworkspace.calendar.EventCreatedTrigger
                    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
                    calendarIds:
                      - "team-calendar@company.com"
                    searchQuery: "meeting"
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
                  - id: log_events
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.events }}"
                    tasks:
                      - id: log_event
                        type: io.kestra.plugin.core.log.Log
                        message: "Event by {{ taskrun.value.organizer.email }}: {{ taskrun.value.summary }}"

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
public class EventCreatedTrigger extends AbstractCalendarTrigger implements PollingTriggerInterface, TriggerOutput<EventCreatedTrigger.Output> {

    private static final int MAX_EVENTS_PER_POLL = 2500; // Google Calendar API maximum

    public enum EventStatus {
        CONFIRMED,
        TENTATIVE,
        CANCELLED
    }

    @Schema(
        title = "Calendar IDs to monitor",
        description = "List of calendar IDs to monitor (e.g., 'primary' for your main calendar, or calendar emails like 'team@company.com'). " +
            "If not specified, defaults to ['primary']."
    )
    protected Property<List<String>> calendarIds;

    @Schema(
        title = "Free-text search filter",
        description = "Only events matching this search term will trigger an execution. " +
            "The search applies to event title, description, and location fields."
    )
    protected Property<String> searchQuery;

    @Schema(
        title = "Organizer email filter",
        description = "Only events organized by this email address will trigger events"
    )
    protected Property<String> organizerEmail;

    @Schema(
        title = "Event status filter",
        description = "Only events with this status will trigger executions."
    )
    protected Property<EventStatus> eventStatus;

    @Schema(
        title = "The polling interval",
        description = "How frequently to check for new events. Must be at least PT1M (1 minute)."
    )
    @Builder.Default
    protected Duration interval = Duration.ofMinutes(5);

    @Schema(
        title = "Maximum number of events to process per poll",
        description = "Limits the number of new events processed in a single poll to avoid overwhelming the system. " +
            "Valid range: 1-" + MAX_EVENTS_PER_POLL + " (Google Calendar API maximum). Default is 100."
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

        // Build output with all events
        Output output = Output.builder()
            .events(allNewEvents)
            .build();

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);
            
        return Optional.of(execution);
    }

    private void validateConfiguration(RunContext runContext) throws Exception {
        // Validate interval minimum
        if (this.interval.toMillis() < Duration.ofMinutes(1).toMillis()) {
            throw new IllegalArgumentException(
                "Polling interval must be at least 1 minute (PT1M)"
            );
        }

        // Validate max events per poll
        Integer rMaxEvents = runContext.render(this.maxEventsPerPoll).as(Integer.class).orElse(100);
        if (rMaxEvents < 1 || rMaxEvents > MAX_EVENTS_PER_POLL) {
            throw new IllegalArgumentException(
                "maxEventsPerPoll must be between 1 and " + MAX_EVENTS_PER_POLL
            );
        }
    }

    protected List<String> getCalendarsToMonitor(RunContext runContext) throws Exception {
        if (this.calendarIds != null) {
            List<String> rCalendars = runContext.render(this.calendarIds).asList(String.class);
            if (!rCalendars.isEmpty()) {
                return rCalendars;
            }
        }
        
        // Default to "primary" - a special Google Calendar API keyword for the user's main calendar
        // See: https://developers.google.com/calendar/api/v3/reference/events/list
        return List.of("primary");
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
        if (this.searchQuery != null) {
            String rQuery = runContext.render(this.searchQuery).as(String.class).orElse(null);
            if (rQuery != null && !rQuery.trim().isEmpty()) {
                request.setQ(rQuery);
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
            .toList();
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
                String rOrganizerEmail = runContext.render(this.organizerEmail).as(String.class).orElse(null);
                if (rOrganizerEmail != null && !rOrganizerEmail.equalsIgnoreCase(event.getOrganizer().getEmail())) {
                    return false;
                }
            }

            // Filter by event status if specified
            if (this.eventStatus != null) {
                EventStatus rEventStatus = runContext.render(this.eventStatus).as(EventStatus.class).orElse(null);
                if (rEventStatus != null && !rEventStatus.name().equalsIgnoreCase(event.getStatus())) {
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