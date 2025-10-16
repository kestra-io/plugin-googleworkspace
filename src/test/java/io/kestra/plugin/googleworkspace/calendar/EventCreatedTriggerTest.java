package io.kestra.plugin.googleworkspace.calendar;

import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.Events;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
@DisabledIf("isServiceAccountNotExists")
class EventCreatedTriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldValidateConfigurationCorrectly() throws Exception {
        // Test valid configuration
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount()))
            .calendarId(Property.ofValue("primary"))
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
    void shouldRejectInvalidInterval() {
        // Test invalid interval if less than 1 minute
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount()))
            .calendarId(Property.ofValue("primary"))
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
    void shouldRejectBothCalendarIdAndCalendarIds() {
        // Test configuration with both calendarId and calendarIds
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount()))
            .calendarId(Property.ofValue("primary"))
            .calendarIds(Property.ofValue(List.of("calendar1@example.com", "calendar2@example.com")))
            .interval(Duration.ofMinutes(5))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .build();
        
        TriggerContext triggerContext = TriggerContext.builder()
            .triggerId(trigger.getId())
            .build();

        // This should throw during evaluation due to invalid configuration
        assertThrows(IllegalArgumentException.class, () -> {
            trigger.evaluate(conditionContext, triggerContext);
        });
    }

    @Test
    void shouldUseDefaultCalendarWhenNoneSpecified() throws Exception {
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount()))
            // No calendar specified - should default to "primary"
            .interval(Duration.ofMinutes(5))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());

        // I test it by making sure the trigger builds correctly
        assertThat(trigger.getCalendarId(), nullValue());
        assertThat(trigger.getCalendarIds(), nullValue());
        assertThat(trigger.getInterval(), equalTo(Duration.ofMinutes(5)));
    }

    @Test
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
        assertThat(trigger.getCalendarId(), nullValue());
    }

    @Test
    void shouldConvertEventMetadataCorrectly() {
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount()))
            .calendarId(Property.ofValue("primary"))
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
    @DisabledIf("isServiceAccountNotExists")
    void shouldConnectToCalendarAPI() throws Exception {
        var trigger = EventCreatedTrigger.builder()
            .id(IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount()))
            .calendarId(Property.ofValue("primary"))
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
        return System.getenv("GOOGLE_SERVICE_ACCOUNT_JSON");
    }
}