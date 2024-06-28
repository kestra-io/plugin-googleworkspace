package io.kestra.plugin.googleworkspace.sheets;

import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.kestra.plugin.googleworkspace.sheets.type.InsertData;
import io.kestra.plugin.googleworkspace.sheets.type.ValueInput;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@MicronautTest
class WriteValueTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void updateValue() throws Exception {
        WriteValue task = WriteValue.builder()
            .id(WriteValueTest.class.getSimpleName())
            .type(WriteValue.class.getName())
            .spreadsheetId("1ZtZnGLVwjTYyt7rFvarLEqy1-Nw6-dn91V34WaLAWvI") //TODO change to a new sheet with limited privileges
            .serviceAccount(UtilsTest.serviceAccount())
            .range("Test1!A1:A1") //TODO change sheet name
            .value("SampleValue")
            .valueInput(ValueInput.RAW)
            .writeOperation(AbstractWrite.WriteOperation.UPDATE)
            .build();

        WriteValue.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUpdatedCells(), is(1));
        assertThat(run.getUpdatedRows(), is(1));
        assertThat(run.getUpdatedColumns(), is(1));
        assertThat(run.getUpdatedRange(), is("Test1!A1"));
    }

    @Test
    void appendValue() throws Exception {
        WriteValue task = WriteValue.builder()
            .id(WriteValueTest.class.getSimpleName())
            .type(WriteValue.class.getName())
            .spreadsheetId("1ZtZnGLVwjTYyt7rFvarLEqy1-Nw6-dn91V34WaLAWvI") //TODO change to a new sheet with limited privileges
            .serviceAccount(UtilsTest.serviceAccount())
            .range("Test1!B1") //TODO change sheet name
            .value("SampleValue")
            .valueInput(ValueInput.RAW)
            .writeOperation(AbstractWrite.WriteOperation.UPDATE)
            .build();

        WriteValue.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUpdatedCells(), is(1));
        assertThat(run.getUpdatedRows(), is(1));
        assertThat(run.getUpdatedColumns(), is(1));
        assertThat(run.getUpdatedRange(), startsWith("Test1!B"));
    }

    @Test
    void updateValuesWithColumnsArrayDirection() throws Exception {
        WriteValue task = WriteValue.builder()
            .id(WriteValueTest.class.getSimpleName())
            .type(WriteValue.class.getName())
            .spreadsheetId("1ZtZnGLVwjTYyt7rFvarLEqy1-Nw6-dn91V34WaLAWvI") //TODO change to a new sheet with limited privileges
            .serviceAccount(UtilsTest.serviceAccount())
            .range("Test2!A1:B1") //TODO change sheet name
            .value(List.of("SampleValue", "SampleSecValue"))
            .valueInput(ValueInput.RAW)
            .arrayDirection(WriteValue.ArrayDirection.ROWS)
            .writeOperation(AbstractWrite.WriteOperation.UPDATE)
            .build();

        WriteValue.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUpdatedCells(), is(2));
        assertThat(run.getUpdatedRows(), is(1));
        assertThat(run.getUpdatedColumns(), is(2));
        assertThat(run.getUpdatedRange(), is("Test2!A1:B1"));
    }

    @Test
    void updateValuesWithRowsArrayDirection() throws Exception {
        WriteValue task = WriteValue.builder()
            .id(WriteValueTest.class.getSimpleName())
            .type(WriteValue.class.getName())
            .spreadsheetId("1ZtZnGLVwjTYyt7rFvarLEqy1-Nw6-dn91V34WaLAWvI") //TODO change to a new sheet with limited privileges
            .serviceAccount(UtilsTest.serviceAccount())
            .range("Test2!A1") //TODO change sheet name
            .value(List.of("SampleValue", "SampleSecValue"))
            .valueInput(ValueInput.RAW)
            .arrayDirection(WriteValue.ArrayDirection.COLUMNS)
            .writeOperation(AbstractWrite.WriteOperation.UPDATE)
            .build();

        WriteValue.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUpdatedCells(), is(2));
        assertThat(run.getUpdatedRows(), is(2));
        assertThat(run.getUpdatedColumns(), is(1));
        assertThat(run.getUpdatedRange(), startsWith("Test2!A"));
    }

    @Test
    void appendValues() throws Exception {
        WriteValue task = WriteValue.builder()
            .id(WriteValueTest.class.getSimpleName())
            .type(WriteValue.class.getName())
            .spreadsheetId("1ZtZnGLVwjTYyt7rFvarLEqy1-Nw6-dn91V34WaLAWvI") //TODO change to a new sheet with limited privileges
            .serviceAccount(UtilsTest.serviceAccount())
            .range("Test2!A1:A1") //TODO change sheet name
            .value(List.of("SampleValue", "SampleSecValue"))
            .writeOperation(AbstractWrite.WriteOperation.APPEND)
            .valueInput(ValueInput.RAW)
            .insertData(InsertData.INSERT_ROWS)
            .build();

        WriteValue.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUpdatedCells(), is(2));
        assertThat(run.getUpdatedRows(), is(1));
        assertThat(run.getUpdatedColumns(), is(2));
    }
}
