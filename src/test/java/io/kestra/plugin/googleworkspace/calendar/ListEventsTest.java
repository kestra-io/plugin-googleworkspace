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
class ListEventsTest {
    @Inject private RunContextFactory runContextFactory;

    @Test
    void listWithRangeAndKeyword() throws Exception {
        RunContext runContext = runContextFactory.of();

        CalendarTime startTime = CalendarTime.builder()
            .dateTime(Property.ofValue("2025-08-13T11:00:00+05:30"))
            .timeZone(Property.ofValue("Asia/Kolkata")).build();

        CalendarTime endTime = CalendarTime.builder()
            .dateTime(Property.ofValue("2025-08-13T11:30:00+05:30"))
            .timeZone(Property.ofValue("Asia/Kolkata")).build();

        String title = "Kestra ListEventsTest – standup";

        InsertEvent insertTask = InsertEvent.builder()
            .calendarId(Property.ofValue("primary"))
            .summary(Property.ofValue(title))
            .description("Event for ListEvents integration test")
            .startTime(startTime)
            .endTime(endTime)
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .build();

        InsertEvent.Output insertOut = insertTask.run(runContext);
        assertThat(insertOut.getEvent().getId(), notNullValue());

        ListEvents listTask = ListEvents.builder()
            .calendarId(Property.ofValue("primary"))
            .timeMin(Property.ofValue("2025-08-13T00:00:00Z"))
            .timeMax(Property.ofValue("2025-08-14T00:00:00Z"))
            .q(Property.ofValue("standup"))
            .singleEvents(Property.ofValue(true))
            .orderBy(Property.ofValue("startTime"))
            .showDeleted(Property.ofValue(false))
            .maxResults(Property.ofValue(250))
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .build();

        ListEvents.Output out = listTask.run(runContext);

        assertThat(out.getEvents(), notNullValue());
        assertThat(out.getEvents().size(), greaterThanOrEqualTo(1));
        assertThat(
            out.getEvents().stream().anyMatch(e -> title.equals(e.getSummary())),
            is(true)
        );
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource(".gcp-service-account.json") == null;
    }
}