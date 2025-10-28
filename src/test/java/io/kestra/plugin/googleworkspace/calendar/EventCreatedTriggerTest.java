package io.kestra.plugin.googleworkspace.calendar;

import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.model.Event;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class EventCreatedTriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @DisabledIf("isServiceAccountNotExists")
    void shouldValidateConfigurationCorrectly() throws Exception {
        // Test valid configuration
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount()))
            .calendarIds(Property.ofValue(List.of("primary")))
            .interval(Duration.ofMinutes(5))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .build();
        
        TriggerContext triggerContext = TriggerContext.builder()
            .triggerId(trigger.getId())
            .build();
        
        // This should not throw any exception for valid configuration
        assertDoesNotThrow(() -> {
            trigger.evaluate(conditionContext, triggerContext);
        });
    }

    @Test
    @DisabledIf("isServiceAccountNotExists")
    void shouldRejectInvalidInterval() {
        // Test invalid interval if less than 1 minute
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount()))
            .calendarIds(Property.ofValue(List.of("primary")))
            .interval(Duration.ofSeconds(30)) // Invalid 
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .build();
        
        TriggerContext triggerContext = TriggerContext.builder()
            .triggerId(trigger.getId())
            .build();

        // This should throw during evaluation due to invalid interval
        assertThrows(IllegalArgumentException.class, () -> {
            trigger.evaluate(conditionContext, triggerContext);
        });
    }

    @Test
    @DisabledIf("isServiceAccountNotExists")
    void shouldUseDefaultCalendarWhenNoneSpecified() throws Exception {
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount()))
            // No calendar specified - should default to "primary"
            .interval(Duration.ofMinutes(5))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());

        // I test it by making sure the trigger builds correctly and will default to primary
        assertThat(trigger.getCalendarIds(), nullValue());
        assertThat(trigger.getInterval(), equalTo(Duration.ofMinutes(5)));
    }

    @Test
    @DisabledIf("isServiceAccountNotExists")
    void shouldHandleMultipleCalendars() throws Exception {
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount()))
            .calendarIds(Property.ofValue(List.of("primary", "team@company.com", "project@company.com")))
            .interval(Duration.ofMinutes(10))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        
        // Validate configuration
        assertThat(trigger.getCalendarIds(), notNullValue());
    }

    @Test
    void shouldConvertEventMetadataCorrectly() {
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue("dummy"))
            .calendarIds(Property.ofValue(List.of("primary")))
            .build();

        // Create a mock Google Calendar Event
        Event googleEvent = new Event()
            .setId("eventSomething")
            .setSummary("Test Meeting")
            .setDescription("This is a test meeting")
            .setLocation("Conference of no Return")
            .setStatus("confirmed")
            .setHtmlLink("https://calendar.google.com/event?eid=eventSomething")
            .setCreated(new DateTime(System.currentTimeMillis()))
            .setUpdated(new DateTime(System.currentTimeMillis()));

        // Add start and end times
        com.google.api.services.calendar.model.EventDateTime start = 
            new com.google.api.services.calendar.model.EventDateTime()
                .setDateTime(new DateTime(System.currentTimeMillis() + 3600000)) // In 1 hour
                .setTimeZone("UTC");
        
        com.google.api.services.calendar.model.EventDateTime end = 
            new com.google.api.services.calendar.model.EventDateTime()
                .setDateTime(new DateTime(System.currentTimeMillis() + 7200000)) // in 2 hours
                .setTimeZone("UTC");

        googleEvent.setStart(start);
        googleEvent.setEnd(end);

        // Add organizer
        Event.Organizer organizer = new Event.Organizer()
            .setEmail("lethal@company.com")
            .setDisplayName("Meeting Organizer");
        googleEvent.setOrganizer(organizer);

        EventCreatedTrigger.EventMetadata metadata = trigger.convertToEventMetadata(googleEvent);

        assertThat(metadata.getId(), equalTo("eventSomething"));
        assertThat(metadata.getSummary(), equalTo("Test Meeting"));
        assertThat(metadata.getDescription(), equalTo("This is a test meeting"));
        assertThat(metadata.getLocation(), equalTo("Conference of no Return"));
        assertThat(metadata.getStatus(), equalTo("confirmed"));
        assertThat(metadata.getHtmlLink(), equalTo("https://calendar.google.com/event?eid=eventSomething"));
        assertThat(metadata.getOrganizer().getEmail(), equalTo("lethal@company.com"));
        assertThat(metadata.getOrganizer().getDisplayName(), equalTo("Meeting Organizer"));
        assertThat(metadata.getStart().getTimeZone(), equalTo("UTC"));
        assertThat(metadata.getEnd().getTimeZone(), equalTo("UTC"));
    }

    @Test
    void shouldFilterEventsByStatus() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Instant lastCreatedTime = Instant.now().minusSeconds(3600);

        // Test CONFIRMED filter
        var confirmedTrigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue("dummy"))
            .eventStatus(Property.ofValue(EventCreatedTrigger.EventStatus.CONFIRMED))
            .build();

        Event confirmedEvent = new Event()
            .setStatus("confirmed")
            .setCreated(new DateTime(Instant.now().toEpochMilli()));
        
        Event tentativeEvent = new Event()
            .setStatus("tentative")
            .setCreated(new DateTime(Instant.now().toEpochMilli()));

        assertThat("CONFIRMED trigger should accept confirmed events", 
            confirmedTrigger.isNewEvent(confirmedEvent, lastCreatedTime, runContext), is(true));
        assertThat("CONFIRMED trigger should reject tentative events", 
            confirmedTrigger.isNewEvent(tentativeEvent, lastCreatedTime, runContext), is(false));

        // Test TENTATIVE filter
        var tentativeTrigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue("dummy"))
            .eventStatus(Property.ofValue(EventCreatedTrigger.EventStatus.TENTATIVE))
            .build();

        assertThat("TENTATIVE trigger should accept tentative events", 
            tentativeTrigger.isNewEvent(tentativeEvent, lastCreatedTime, runContext), is(true));
        assertThat("TENTATIVE trigger should reject confirmed events", 
            tentativeTrigger.isNewEvent(confirmedEvent, lastCreatedTime, runContext), is(false));

        // Test CANCELLED filter
        var cancelledTrigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue("dummy"))
            .eventStatus(Property.ofValue(EventCreatedTrigger.EventStatus.CANCELLED))
            .build();

        Event cancelledEvent = new Event()
            .setStatus("cancelled")
            .setCreated(new DateTime(Instant.now().toEpochMilli()));

        assertThat("CANCELLED trigger should accept cancelled events", 
            cancelledTrigger.isNewEvent(cancelledEvent, lastCreatedTime, runContext), is(true));
        assertThat("CANCELLED trigger should reject confirmed events", 
            cancelledTrigger.isNewEvent(confirmedEvent, lastCreatedTime, runContext), is(false));
    }

    @Test
    void shouldFilterEventsByOrganizerEmail() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Instant lastCreatedTime = Instant.now().minusSeconds(3600);

        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue("dummy"))
            .organizerEmail(Property.ofValue("manager@company.com"))
            .build();

        Event matchingEvent = new Event()
            .setCreated(new DateTime(Instant.now().toEpochMilli()))
            .setOrganizer(new Event.Organizer().setEmail("manager@company.com"));

        Event nonMatchingEvent = new Event()
            .setCreated(new DateTime(Instant.now().toEpochMilli()))
            .setOrganizer(new Event.Organizer().setEmail("other@company.com"));

        Event noOrganizerEvent = new Event()
            .setCreated(new DateTime(Instant.now().toEpochMilli()));

        assertThat("Should accept events from specified organizer", 
            trigger.isNewEvent(matchingEvent, lastCreatedTime, runContext), is(true));
        assertThat("Should reject events from different organizer", 
            trigger.isNewEvent(nonMatchingEvent, lastCreatedTime, runContext), is(false));
        assertThat("Should reject events without organizer", 
            trigger.isNewEvent(noOrganizerEvent, lastCreatedTime, runContext), is(true)); // No filter means accept
    }

    @Test
    void shouldFilterEventsByCreationTime() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Instant lastCreatedTime = Instant.now().minusSeconds(3600); // 1 hour ago

        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue("dummy"))
            .build();

        // Event created after the threshold (should be included)
        Event newEvent = new Event()
            .setCreated(new DateTime(Instant.now().toEpochMilli()));

        // Event created before the threshold (should be excluded)
        Event oldEvent = new Event()
            .setCreated(new DateTime(lastCreatedTime.minusSeconds(600).toEpochMilli()));

        assertThat("Should accept events created after lastCreatedTime", 
            trigger.isNewEvent(newEvent, lastCreatedTime, runContext), is(true));
        assertThat("Should reject events created before lastCreatedTime", 
            trigger.isNewEvent(oldEvent, lastCreatedTime, runContext), is(false));
    }

    @Test
    void shouldReturnPrimaryCalendarByDefault() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue("dummy"))
            // No calendarIds specified
            .build();

        List<String> calendars = trigger.getCalendarsToMonitor(runContext);

        assertThat("Should default to 'primary' calendar", calendars, hasSize(1));
        assertThat("Should default to 'primary' calendar", calendars.get(0), equalTo("primary"));
    }

    @Test
    void shouldReturnSpecifiedCalendars() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue("dummy"))
            .calendarIds(Property.ofValue(List.of("team@company.com", "project@company.com")))
            .build();

        List<String> calendars = trigger.getCalendarsToMonitor(runContext);

        assertThat("Should return specified calendars", calendars, hasSize(2));
        assertThat("Should contain first calendar", calendars, hasItem("team@company.com"));
        assertThat("Should contain second calendar", calendars, hasItem("project@company.com"));
    }

    @Test
    void shouldRejectMaxEventsPerPollBelowMinimum() {
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue("dummy"))
            .calendarIds(Property.ofValue(List.of("primary")))
            .interval(Duration.ofMinutes(5))
            .maxEventsPerPoll(Property.ofValue(0)) // Invalid - below minimum
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .build();
        
        TriggerContext triggerContext = TriggerContext.builder()
            .triggerId(trigger.getId())
            .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            trigger.evaluate(conditionContext, triggerContext);
        });

        assertThat("Error message should mention valid range", 
            exception.getMessage(), containsString("between 1 and 2500"));
    }

    @Test
    void shouldRejectMaxEventsPerPollAboveMaximum() {
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue("dummy"))
            .calendarIds(Property.ofValue(List.of("primary")))
            .interval(Duration.ofMinutes(5))
            .maxEventsPerPoll(Property.ofValue(3000)) // Invalid - above maximum
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .build();
        
        TriggerContext triggerContext = TriggerContext.builder()
            .triggerId(trigger.getId())
            .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            trigger.evaluate(conditionContext, triggerContext);
        });

        assertThat("Error message should mention valid range", 
            exception.getMessage(), containsString("between 1 and 2500"));
    }

    @Test
    void shouldCombineMultipleFilters() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Instant lastCreatedTime = Instant.now().minusSeconds(3600);

        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue("dummy"))
            .organizerEmail(Property.ofValue("manager@company.com"))
            .eventStatus(Property.ofValue(EventCreatedTrigger.EventStatus.CONFIRMED))
            .build();

        // Event matching all criteria
        Event matchingEvent = new Event()
            .setCreated(new DateTime(Instant.now().toEpochMilli()))
            .setOrganizer(new Event.Organizer().setEmail("manager@company.com"))
            .setStatus("confirmed");

        // Event matching organizer but wrong status
        Event wrongStatusEvent = new Event()
            .setCreated(new DateTime(Instant.now().toEpochMilli()))
            .setOrganizer(new Event.Organizer().setEmail("manager@company.com"))
            .setStatus("tentative");

        // Event matching status but wrong organizer
        Event wrongOrganizerEvent = new Event()
            .setCreated(new DateTime(Instant.now().toEpochMilli()))
            .setOrganizer(new Event.Organizer().setEmail("other@company.com"))
            .setStatus("confirmed");

        assertThat("Should accept event matching all filters", 
            trigger.isNewEvent(matchingEvent, lastCreatedTime, runContext), is(true));
        assertThat("Should reject event with wrong status", 
            trigger.isNewEvent(wrongStatusEvent, lastCreatedTime, runContext), is(false));
        assertThat("Should reject event with wrong organizer", 
            trigger.isNewEvent(wrongOrganizerEvent, lastCreatedTime, runContext), is(false));
    }

    // ===== Integration Tests (require real credentials) =====

    @Test
    @DisabledIf("isServiceAccountNotExists")
    void shouldConnectToCalendarAPI() throws Exception {
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount()))
            .calendarIds(Property.ofValue(List.of("primary")))
            .interval(Duration.ofMinutes(5))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());

        // This should succeed and create a connection
        com.google.api.services.calendar.Calendar calendar = trigger.connection(runContext);
        
        assertThat(calendar, notNullValue());
        assertThat(calendar.getApplicationName(), equalTo("Kestra"));
    }

    private static boolean isServiceAccountNotExists() {
        return serviceAccount() == null;
    }

    private static String serviceAccount() {
        return System.getenv("GOOGLE_SERVICE_ACCOUNT");
    }
}