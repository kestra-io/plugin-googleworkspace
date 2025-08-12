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

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
class GetEventTest {
    @Inject private RunContextFactory runContextFactory;

    @Test
    void getSingleEventById() throws Exception {
        RunContext runContext = runContextFactory.of();

        CalendarTime startTime = CalendarTime.builder()
            .dateTime(Property.ofValue("2025-08-12T09:00:00+05:30"))
            .timeZone(Property.ofValue("Asia/Kolkata")).build();

        CalendarTime endTime = CalendarTime.builder()
            .dateTime(Property.ofValue("2025-08-12T09:30:00+05:30"))
            .timeZone(Property.ofValue("Asia/Kolkata")).build();

        InsertEvent insertTask = InsertEvent.builder()
            .calendarId(Property.ofValue("primary"))
            .summary(Property.ofValue("Kestra GetEventTest"))
            .description("event for GetEvent test")
            .startTime(startTime)
            .endTime(endTime)
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .build();

        InsertEvent.Output insertOut = insertTask.run(runContext);
        assertThat(insertOut.getEvent().getId(), notNullValue());

        GetEvent getTask = GetEvent.builder()
            .calendarId(Property.ofValue("primary"))
            .eventId(Property.ofValue(insertOut.getEvent().getId()))
            .maxAttendees(Property.ofValue(10))
            .alwaysIncludeEmail(Property.ofValue(true))
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .build();

        GetEvent.Output getOut = getTask.run(runContext);

        assertThat(getOut.getEvent(), notNullValue());
        assertThat(getOut.getEvent().getId(), is(insertOut.getEvent().getId()));
        assertThat(getOut.getEvent().getSummary(), is("Kestra GetEventTest"));
        assertThat(getOut.getEvent().getStatus(), anyOf(is("confirmed"), is("tentative")));
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource(".gcp-service-account.json") == null;
    }
}
