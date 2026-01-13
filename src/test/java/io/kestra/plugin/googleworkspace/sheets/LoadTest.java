package io.kestra.plugin.googleworkspace.sheets;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
@Execution(ExecutionMode.SAME_THREAD)
class LoadTest {
    private static final AtomicInteger ROW_CURSOR = new AtomicInteger(1);

    @Inject
    private RunContextFactory runContextFactory;

    private static RunContext runContext;

    private static String spreadsheetId;

    @Inject
    private StorageInterface storageInterface;

    private static String serviceAccount;

    @BeforeAll
    static void intiAccount() throws Exception {
        serviceAccount = UtilsTest.serviceAccount();
    }

    @BeforeAll
    static void setup(RunContextFactory runContextFactory) throws Exception {
        runContext = runContextFactory.of();

        CreateSpreadsheet createTask = CreateSpreadsheet.builder()
            .id("shared-spreadsheet")
            .title(Property.ofValue("Kestra Integration Test"))
            .serviceAccount(Property.ofValue(serviceAccount))
            .build();

        spreadsheetId = createTask.run(runContext).getSpreadsheetId();
    }

    @Test
    void loadCSV() throws Exception {
        RunContext runContext = runContextFactory.of();

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue(nextRange()))
            .insertType(Property.ofValue(Load.InsertType.APPEND))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(getSource(".csv").toString()))
            .build();

        var run = task.run(runContext);

        assertThat(run.getRows(), is(6));
        assertThat(run.getColumns(), is(6));
    }

    @Test
    void loadJSON() throws Exception {
        RunContext runContext = runContextFactory.of();

        URI source = getSource(".json");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue(nextRange()))
            .insertType(Property.ofValue(Load.InsertType.APPEND))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source.toString()))
            .build();

        var run = task.run(runContext);

        assertThat(run.getRows(), is(6));
        assertThat(run.getColumns(), is(6));
    }

    @Test
    void loadJSONWithHeader() throws Exception {
        RunContext runContext = runContextFactory.of();
        URI source = getSource(".json");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue(nextRange()))
            .insertType(Property.ofValue(Load.InsertType.APPEND))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source.toString()))
            .header(Property.ofValue(true))
            .build();

        var run = task.run(runContext);

        assertThat(run.getRows(), is(greaterThan(6)));
        assertThat(run.getColumns(), is(6));
    }

    @Test
    void loadAVRO() throws Exception {
        RunContext runContext = runContextFactory.of();;

        URI source = getSource(".avro");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue(nextRange()))
            .insertType(Property.ofValue(Load.InsertType.APPEND))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source.toString()))
            .build();

        var run = task.run(runContext);

        assertThat(run.getRows(), is(6));
        assertThat(run.getColumns(), is(6));
    }

    @Test
    void loadAVROWithHeader() throws Exception {
        RunContext runContext = runContextFactory.of();

        URI source = getSource(".avro");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue(nextRange()))
            .insertType(Property.ofValue(Load.InsertType.APPEND))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source.toString()))
            .header(Property.ofValue(true))
            .build();

        var run = task.run(runContext);

        assertThat(run.getRows(), is(greaterThan(6)));
        assertThat(run.getColumns(), is(6));
    }

    @Test
    void loadORC() throws Exception {
        RunContext runContext = runContextFactory.of();

        URI source = getSource(".orc");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue(nextRange()))
            .insertType(Property.ofValue(Load.InsertType.APPEND))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source.toString()))
            .build();

        var run = task.run(runContext);

        assertThat(run.getRows(), is(6));
        assertThat(run.getColumns(), is(6));
    }

    @Test
    void loadORCWithHeader() throws Exception {
        RunContext runContext = runContextFactory.of();
        URI source = getSource(".orc");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue(nextRange()))
            .insertType(Property.ofValue(Load.InsertType.APPEND))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source.toString()))
            .header(Property.ofValue(true))
            .build();

        var run = task.run(runContext);

        assertThat(run.getRows(), is(greaterThan(6)));
        assertThat(run.getColumns(), is(6));
    }

    @Test
    void loadPARQUET() throws Exception {
        RunContext runContext = runContextFactory.of();
        URI source = getSource(".parquet");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue(nextRange()))
            .insertType(Property.ofValue(Load.InsertType.APPEND))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source.toString()))
            .build();

        var run = task.run(runContext);

        assertThat(run.getRows(), is(6));
        assertThat(run.getColumns(), is(6));
    }

    @Test
    void loadPARQUETWithHeader() throws Exception {
        RunContext runContext = runContextFactory.of();
        URI source = getSource(".parquet");

        Load task = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue(nextRange()))
            .insertType(Property.ofValue(Load.InsertType.APPEND))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source.toString()))
            .header(Property.ofValue(true))
            .build();

        var run = task.run(runContext);

        assertThat(run.getRows(), is(greaterThan(6)));
        assertThat(run.getColumns(), is(6));
    }

    @Test
    void loadWithOverwriteMode() throws Exception {
        RunContext runContext = runContextFactory.of();
        String spreadsheetId = createSpreadsheet(runContext);

        URI source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create() + ".csv"),
            new FileInputStream(new File(Objects.requireNonNull(LoadTest.class.getClassLoader()
                    .getResource("examples/addresses.csv"))
                .toURI()))
        );

        URI source2 = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create() + ".csv"),
            new FileInputStream(new File(Objects.requireNonNull(LoadTest.class.getClassLoader()
                    .getResource("examples/addresses-small.csv"))
                .toURI()))
        );

        Load load1 = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue("Sheet1!A1:F6"))
            .insertType(Property.ofValue(Load.InsertType.OVERWRITE))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source.toString()))
            .insertType(Property.ofValue(Load.InsertType.OVERWRITE))
            .build();

        var out1 = load1.run(runContext);

        assertThat(out1.getRows(), is(notNullValue()));
        assertThat(out1.getColumns(), is(notNullValue()));

        Load load2 = Load.builder()
            .id("overwrite_small")
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue(nextRange()))
            .range(Property.ofValue("Sheet1!A1:F6"))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source2.toString()))
            .insertType(Property.ofValue(Load.InsertType.OVERWRITE))
            .build();

        var out2 = load2.run(runContext);

        assertThat(out2.getRows(), is(notNullValue()));
        assertThat(out2.getColumns(), is(notNullValue()));

        deleteSpreadsheet(runContext, spreadsheetId);
    }

    @Test
    void loadWithAppendMode() throws Exception {
        RunContext runContext = runContextFactory.of();
        String spreadsheetId = createSpreadsheet(runContext);

        URI source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create() + ".csv"),
            new FileInputStream(new File(Objects.requireNonNull(LoadTest.class.getClassLoader()
                    .getResource("examples/addresses.csv"))
                .toURI()))
        );

        URI source2 = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create() + ".csv"),
            new FileInputStream(new File(Objects.requireNonNull(LoadTest.class.getClassLoader()
                    .getResource("examples/addresses-small.csv"))
                .toURI()))
        );

        Load load1 = Load.builder()
            .id("load_append_ " + IdUtils.create())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue("Sheet1"))
            .insertType(Property.ofValue(Load.InsertType.APPEND))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source.toString()))
            .build();

        var out1 = load1.run(runContext);

        assertThat(out1.getRows(), is(notNullValue()));
        assertThat(out1.getColumns(), is(notNullValue()));

        Load load2 = Load.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .range(Property.ofValue("Sheet1"))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .from(Property.ofValue(source2.toString()))
            .insertType(Property.ofValue(Load.InsertType.APPEND))
            .build();

        var out2 = load2.run(runContext);

        assertThat(out2.getRows(), is(notNullValue()));
        assertThat(out2.getColumns(), is(notNullValue()));

        Read read = Read.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .selectedSheetsTitle(Property.ofValue(List.of("Sheet1")))
            .fetch(Property.ofValue(true))
            .build();

        Read.Output out = read.run(runContext);

        assertThat(out.getSize(), is(9));
        assertThat(out.getRows().containsKey("Sheet1"), is(true));

        deleteSpreadsheet(runContext, spreadsheetId);
    }

    private URI getSource(String extension) throws IOException, URISyntaxException {
        URL resource = LoadTest.class.getClassLoader().getResource("examples/addresses" + extension);

        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create() + extension),
            new FileInputStream(new File(Objects.requireNonNull(resource)
                .toURI()))
        );
    }

    private String createSpreadsheet(RunContext runContext) throws Exception {
        CreateSpreadsheet createTask = CreateSpreadsheet.builder()
            .id(LoadTest.class.getSimpleName())
            .title(Property.ofValue("CSV Test Spreadsheet"))
            .serviceAccount(Property.ofValue(serviceAccount))
            .build();

        CreateSpreadsheet.Output createOutput = createTask.run(runContext);

        assertThat(createOutput.getSpreadsheetId(), is(notNullValue()));

        return createOutput.getSpreadsheetId();
    }

    private void deleteSpreadsheet(RunContext runContext, String spreadsheetId) throws Exception {
        DeleteSpreadsheet deleteTask = DeleteSpreadsheet.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .build();

        DeleteSpreadsheet.Output deleteOutput = deleteTask.run(runContext);

        assertThat(deleteOutput.getSpreadsheetId(), is(notNullValue()));
    }

    @AfterAll
    static void cleanup() throws Exception {
        DeleteSpreadsheet deleteTask = DeleteSpreadsheet.builder()
            .id(LoadTest.class.getSimpleName())
            .serviceAccount(Property.ofValue(serviceAccount))
            .spreadsheetId(Property.ofValue(spreadsheetId))
            .build();

        DeleteSpreadsheet.Output deleteOutput = deleteTask.run(runContext);

        assertThat(deleteOutput.getSpreadsheetId(), is(notNullValue()));
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource(".gcp-service-account.json") == null;
    }

    private String nextRange() {
        int start = ROW_CURSOR.getAndAdd(20);
        return "Sheet1!A" + start;
    }
}
