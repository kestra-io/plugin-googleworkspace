package io.kestra.plugin.googleworkspace.sheets;

import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.kestra.plugin.googleworkspace.sheets.type.ValueInput;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@MicronautTest
public class WriteFileTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void writeFile() throws Exception {
        URI source = storageInterface.put(
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(new File(Objects.requireNonNull(WriteFileTest.class.getClassLoader()
                    .getResource("examples/addresses3.csv"))
                .toURI()))
        );

        WriteFile task = WriteFile.builder()
            .id(WriteFileTest.class.getSimpleName())
            .type(WriteFile.class.getName())
            .spreadsheetId("1ZtZnGLVwjTYyt7rFvarLEqy1-Nw6-dn91V34WaLAWvI") //TODO change to a new sheet with limited privileges
            .serviceAccount(UtilsTest.serviceAccount())
            .range("Test3!A1") //TODO change sheet name
            .from(source.toString())
            .valueInput(ValueInput.RAW)
            .writeOperation(AbstractWrite.WriteOperation.UPDATE)
            .build();

        WriteFile.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUpdatedCells(), is(2));
        assertThat(run.getUpdatedRows(), is(2));
        assertThat(run.getUpdatedColumns(), is(1));
        assertThat(run.getUpdatedRange(), startsWith("Test3!A1"));
    }

    @Test
    void writeFileWithDataSeparation() throws Exception {
        URI source = storageInterface.put(
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(new File(Objects.requireNonNull(WriteFileTest.class.getClassLoader()
                    .getResource("examples/addresses3.csv"))
                .toURI()))
        );

        WriteFile task = WriteFile.builder()
            .id(WriteFileTest.class.getSimpleName())
            .type(WriteFile.class.getName())
            .spreadsheetId("1ZtZnGLVwjTYyt7rFvarLEqy1-Nw6-dn91V34WaLAWvI") //TODO change to a new sheet with limited privileges
            .serviceAccount(UtilsTest.serviceAccount())
            .range("Test3!A1") //TODO change sheet name
            .from(source.toString())
            .valueInput(ValueInput.RAW)
            .writeOperation(AbstractWrite.WriteOperation.UPDATE)
            .dataSeparator(",")
            .build();

        WriteFile.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUpdatedCells(), is(12));
        assertThat(run.getUpdatedRows(), is(2));
        assertThat(run.getUpdatedColumns(), is(6));
        assertThat(run.getUpdatedRange(), startsWith("Test3!A1"));
    }
}
