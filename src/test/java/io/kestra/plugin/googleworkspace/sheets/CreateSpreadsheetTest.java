package io.kestra.plugin.googleworkspace.sheets;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.googleworkspace.UtilsTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
class CreateSpreadsheetTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        RunContext runContext = runContextFactory.of();

        CreateSpreadsheet createTask = CreateSpreadsheet.builder()
            .id(CreateSpreadsheetTest.class.getSimpleName())
            .title("CSV Test Spreadsheet")
            .serviceAccount(UtilsTest.serviceAccount())
            .build();

        CreateSpreadsheet.Output createOutput = createTask.run(runContext);

        assertThat(createOutput.getSpreadsheetId(), is(notNullValue()));
        assertThat(createOutput.getSpreadsheetUrl(), is(notNullValue()));

        DeleteSpreadsheet deleteTask = DeleteSpreadsheet.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(UtilsTest.serviceAccount())
            .spreadsheetId(createOutput.getSpreadsheetId())
            .build();

        DeleteSpreadsheet.Output deleteOutput = deleteTask.run(runContext);

        assertThat(deleteOutput.getSpreadsheetId(), is(notNullValue()));
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource(".gcp-service-account.json") == null;
    }
}
