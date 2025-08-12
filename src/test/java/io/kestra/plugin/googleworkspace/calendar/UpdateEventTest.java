package io.kestra.plugin.googleworkspace.calendar;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.kestra.plugin.googleworkspace.calendar.AbstractInsertEvent.CalendarTime;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.time.OffsetDateTime;
import java.time.ZoneId;

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
class UpdateEventTest {
    @Inject private RunContextFactory runContextFactory;

    @Test
    void patchTitleAndTime() throws Exception {
        RunContext runContext = runContextFactory.of();

        CalendarTime startTime = CalendarTime.builder()
            .dateTime(Property.ofValue("2025-08-14T09:00:00+05:30"))
            .timeZone(Property.ofValue("Asia/Kolkata")).build();

        CalendarTime endTime = CalendarTime.builder()
            .dateTime(Property.ofValue("2025-08-14T09:30:00+05:30"))
            .timeZone(Property.ofValue("Asia/Kolkata")).build();

        InsertEvent insertTask = InsertEvent.builder()
            .calendarId(Property.ofValue("primary"))
            .summary(Property.ofValue("Kestra UpdateEventTest"))
            .description("Event for UpdateEvent integration test")
            .startTime(startTime)
            .endTime(endTime)
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .build();

        InsertEvent.Output insertOut = insertTask.run(runContext);

        CalendarTime newStart = CalendarTime.builder()
            .dateTime(Property.ofValue("2025-08-14T09:30:00+05:30"))
            .timeZone(Property.ofValue("Asia/Kolkata")).build();

        CalendarTime newEnd = CalendarTime.builder()
            .dateTime(Property.ofValue("2025-08-14T10:00:00+05:30"))
            .timeZone(Property.ofValue("Asia/Kolkata")).build();

        UpdateEvent updateTask = UpdateEvent.builder()
            .calendarId(Property.ofValue("primary"))
            .eventId(Property.ofValue(insertOut.getEvent().getId()))
            .patch(Property.ofValue(true))
            .sendUpdates(Property.ofValue("none"))
            .summary(Property.ofValue("Kestra UpdateEventTest (patched)"))
            .startTime(newStart)
            .endTime(newEnd)
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .build();

        UpdateEvent.Output out = updateTask.run(runContext);

        assertThat(out.getEvent(), notNullValue());
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource(".gcp-service-account.json") == null;
    }
}
