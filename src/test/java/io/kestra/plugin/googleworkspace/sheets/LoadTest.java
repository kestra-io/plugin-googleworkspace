package io.kestra.plugin.googleworkspace.sheets;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
class LoadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void loadCSV() throws Exception {
        RunContext runContext = runContextFactory.of();

        String spreadsheetId = createSpreadsheet(runContext);

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(UtilsTest.serviceAccount())
            .spreadsheetId(spreadsheetId)
            .from(getSource(".csv").toString())
            .build();

        Load.Output run = task.run(runContext);

        assertThat(run.getRows(), is(6));
        assertThat(run.getColumns(), is(6));

        deleteSpreadsheet(runContext, spreadsheetId);
    }

    @Test
    void loadJSON() throws Exception {
        RunContext runContext = runContextFactory.of();

        String spreadsheetId = createSpreadsheet(runContext);

        URI source = getSource(".json");
        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(UtilsTest.serviceAccount())
            .from(source.toString())
            .build();

        Load.Output run = task.run(runContext);

        assertThat(run.getRows(), is(6));
        assertThat(run.getColumns(), is(6));

        deleteSpreadsheet(runContext, spreadsheetId);
    }

    @Test
    void loadJSONWithHeader() throws Exception {
        RunContext runContext = runContextFactory.of();

        String spreadsheetId = createSpreadsheet(runContext);

        URI source = getSource(".json");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(UtilsTest.serviceAccount())
            .spreadsheetId(spreadsheetId)
            .from(source.toString())
            .header(true)
            .build();

        Load.Output run = task.run(runContext);

        assertThat(run.getRows(), is(greaterThan(6)));
        assertThat(run.getColumns(), is(6));

        deleteSpreadsheet(runContext, spreadsheetId);
    }

    @Test
    void loadAVRO() throws Exception {
        RunContext runContext = runContextFactory.of();

        String spreadsheetId = createSpreadsheet(runContext);

        URI source = getSource(".avro");
        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(UtilsTest.serviceAccount())
            .spreadsheetId(spreadsheetId)
            .from(source.toString())
            .build();

        Load.Output run = task.run(runContext);

        assertThat(run.getRows(), is(6));
        assertThat(run.getColumns(), is(6));

        deleteSpreadsheet(runContext, spreadsheetId);
    }

    @Test
    void loadAVROWithHeader() throws Exception {
        RunContext runContext = runContextFactory.of();

        String spreadsheetId = createSpreadsheet(runContext);

        URI source = getSource(".avro");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(UtilsTest.serviceAccount())
            .spreadsheetId(spreadsheetId)
            .from(source.toString())
            .header(true)
            .build();

        Load.Output run = task.run(runContext);

        assertThat(run.getRows(), is(greaterThan(6)));
        assertThat(run.getColumns(), is(6));

        deleteSpreadsheet(runContext, spreadsheetId);
    }

    @Test
    void loadORC() throws Exception {
        RunContext runContext = runContextFactory.of();

        String spreadsheetId = createSpreadsheet(runContext);

        URI source = getSource(".orc");
        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(UtilsTest.serviceAccount())
            .spreadsheetId(spreadsheetId)
            .from(source.toString())
            .build();

        Load.Output run = task.run(runContext);

        assertThat(run.getRows(), is(6));
        assertThat(run.getColumns(), is(6));

        deleteSpreadsheet(runContext, spreadsheetId);
    }

    @Test
    void loadORCWithHeader() throws Exception {
        RunContext runContext = runContextFactory.of();

        String spreadsheetId = createSpreadsheet(runContext);

        URI source = getSource(".orc");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(UtilsTest.serviceAccount())
            .spreadsheetId(spreadsheetId)
            .from(source.toString())
            .header(true)
            .build();

        Load.Output run = task.run(runContext);

        assertThat(run.getRows(), is(greaterThan(6)));
        assertThat(run.getColumns(), is(6));

        deleteSpreadsheet(runContext, spreadsheetId);
    }

    @Test
    void loadPARQUET() throws Exception {
        RunContext runContext = runContextFactory.of();

        String spreadsheetId = createSpreadsheet(runContext);

        URI source = getSource(".parquet");
        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(UtilsTest.serviceAccount())
            .spreadsheetId(spreadsheetId)
            .from(source.toString())
            .build();

        Load.Output run = task.run(runContext);

        assertThat(run.getRows(), is(6));
        assertThat(run.getColumns(), is(6));

        deleteSpreadsheet(runContext, spreadsheetId);
    }

    @Test
    void loadPARQUETWithHeader() throws Exception {
        RunContext runContext = runContextFactory.of();

        String spreadsheetId = createSpreadsheet(runContext);

        URI source = getSource(".parquet");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(UtilsTest.serviceAccount())
            .spreadsheetId(spreadsheetId)
            .from(source.toString())
            .header(true)
            .build();

        Load.Output run = task.run(runContext);

        assertThat(run.getRows(), is(greaterThan(6)));
        assertThat(run.getColumns(), is(6));

        deleteSpreadsheet(runContext, spreadsheetId);
    }

    private URI getSource(String extension) throws IOException, URISyntaxException {
        URL resource = LoadTest.class.getClassLoader().getResource("examples/addresses"+extension);

        return storageInterface.put(
            null,
            new URI("/" + IdUtils.create() + extension),
            new FileInputStream(new File(Objects.requireNonNull(resource)
                .toURI()))
        );
    }

    private String createSpreadsheet(RunContext runContext) throws Exception {
        CreateSpreadsheet createTask = CreateSpreadsheet.builder()
            .id(LoadTest.class.getSimpleName())
            .title("CSV Test Spreadsheet")
            .serviceAccount(UtilsTest.serviceAccount())
            .build();

        CreateSpreadsheet.Output createOutput = createTask.run(runContext);

        assertThat(createOutput.getSpreadsheetId(), is(notNullValue()));

	    return createOutput.getSpreadsheetId();
    }

    private void deleteSpreadsheet(RunContext runContext, String spreadsheetId) throws Exception {
        DeleteSpreadsheet deleteTask = DeleteSpreadsheet.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(UtilsTest.serviceAccount())
            .spreadsheetId(spreadsheetId)
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
