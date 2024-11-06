package io.kestra.plugin.googleworkspace.calendar;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.model.EventDateTime;
import com.google.api.services.calendar.model.Event.Creator;
import com.google.common.base.Strings;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.kestra.plugin.googleworkspace.calendar.AbstractInsertEvent.CalendarTime;
import jakarta.inject.Inject;

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
class InsertEventTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        CalendarTime startTime = CalendarTime.builder().dateTime("2024-11-28T09:00:00+05:30").timeZone("Asia/Calcutta").build();
        CalendarTime endTime = CalendarTime.builder().dateTime("2024-11-28T10:00:00+05:30").timeZone("Asia/Calcutta").build();

        InsertEvent task = InsertEvent.builder()
            .calendarId("primary")
            .summary("New Calendar Event")
            .description("This is a new calendar event generated by Kestra.")
            .startTime(startTime)
            .endTime(endTime)
            .serviceAccount(UtilsTest.serviceAccount())
            .build();

        InsertEvent.Output runOutput = task.run(runContext);

        assertThat(runOutput.getEvent().getId(), is(notNullValue()));
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource(".gcp-service-account.json") == null;
    }
}
