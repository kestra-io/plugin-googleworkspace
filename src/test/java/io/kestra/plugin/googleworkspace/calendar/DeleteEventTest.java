package io.kestra.plugin.googleworkspace.calendar;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.kestra.plugin.googleworkspace.calendar.AbstractInsertEvent.CalendarTime;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
class DeleteEventTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldDeleteEvent() throws Exception {
        RunContext runContext = runContextFactory.of();

        CalendarTime startTime = CalendarTime.builder()
            .dateTime(Property.ofValue("2025-08-15T12:00:00+05:30"))
            .timeZone(Property.ofValue("Asia/Kolkata")).build();

        CalendarTime endTime = CalendarTime.builder()
            .dateTime(Property.ofValue("2025-08-15T12:30:00+05:30"))
            .timeZone(Property.ofValue("Asia/Kolkata")).build();

        InsertEvent insertTask = InsertEvent.builder()
            .calendarId(Property.ofValue("primary"))
            .summary(Property.ofValue("Kestra DeleteEventTest"))
            .description("Event for DeleteEvent integration test")
            .startTime(startTime)
            .endTime(endTime)
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .build();

        InsertEvent.Output insertOut = insertTask.run(runContext);
        String eventId = insertOut.getEvent().getId();
        assertThat(eventId, notNullValue());

        DeleteEvent delete = DeleteEvent.builder()
            .calendarId(Property.ofValue("primary"))
            .eventId(Property.ofValue(eventId))
            .sendUpdates(Property.ofValue("none"))
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .build();

        delete.run(runContext);
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource(".gcp-service-account.json") == null;
    }
}