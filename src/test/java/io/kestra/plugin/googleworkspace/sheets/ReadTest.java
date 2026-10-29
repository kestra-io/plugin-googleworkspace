package io.kestra.plugin.googleworkspace.sheets;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.BatchGetValuesResponse;
import com.google.api.services.sheets.v4.model.GridProperties;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
class ReadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        Read task = Read.builder()
            .id(ReadTest.class.getSimpleName())
            .type(ReadRange.class.getName())
            .spreadsheetId(Property.ofValue("1Dkd7W0OQo-wxz9rrORLP7YGSj6EBLEg73fiTdbJUIQE"))
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .fetch(Property.ofValue(true))
            .build();

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getSize(), is(93));
        assertThat(((Map<String, Object>) run.getRows().get("Class Data").get(6)).get("Date"), is("7/11/2012"));
        assertThat(((Map<String, Object>) run.getRows().get("Second One").get(0)).get("Formula"), is("Female"));
        assertThat(((Map<String, Object>) run.getRows().get("2 Tables").get(0)).get("Student Name"), is("Alexandra"));
        assertThat(((Map<String, Object>) run.getRows().get("2 Tables").get(0)).size(), is(5));
        assertThat(((Map<String, Object>) run.getRows().get("2 Tables").get(25)).get("Student Name"), is("Robert"));
        assertThat(((Map<String, Object>) run.getRows().get("2 Tables").get(25)).size(), is(5));
        assertThat(((Map<String, Object>) run.getRows().get("2 Tables").get(25)).get("Student Name 2"), is(nullValue()));
    }

    @SuppressWarnings("unchecked")
    @Test
    void selected() throws Exception {
        Read task = Read.builder()
            .id(ReadTest.class.getSimpleName())
            .type(ReadRange.class.getName())
            .spreadsheetId(Property.ofValue("1Dkd7W0OQo-wxz9rrORLP7YGSj6EBLEg73fiTdbJUIQE"))
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .selectedSheetsTitle(Property.ofValue(List.of("Second One")))
            .fetch(Property.ofValue(true))
            .build();

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getRows().size(), is(1));
        assertThat(run.getSize(), is(31));
        assertThat(((Map<String, Object>) run.getRows().get("Second One").get(0)).get("Formula"), is("Female"));
        assertThat(((Map<String, Object>) run.getRows().get("Second One").get(0)).size(), is(3));
    }

    @Test
    void emptySheet() throws Exception {
        var spreadsheet = new Spreadsheet()
            .setSheets(
                List.of(
                    new Sheet().setProperties(
                        new SheetProperties()
                            .setTitle("EmptySheet")
                            .setGridProperties(new GridProperties().setRowCount(10).setColumnCount(5))
                    )
                )
            );

        var batchResponse = new BatchGetValuesResponse()
            .setValueRanges(
                List.of(
                    new ValueRange().setRange("EmptySheet!R1C1:R10C5").setValues(null)
                )
            );

        var sheets = mockSheets(spreadsheet, batchResponse);

        var task = Mockito.spy(
            Read.builder()
                .id(IdUtils.create())
                .type(Read.class.getName())
                .spreadsheetId(Property.ofValue("mockSpreadsheetId"))
                .fetch(Property.ofValue(true))
                .build()
        );

        Mockito.doReturn(sheets).when(task).connection(Mockito.any());

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run, notNullValue());
        assertThat(run.getSize(), is(0));
        assertThat(run.getRows().containsKey("EmptySheet"), is(true));
        assertThat(run.getRows().get("EmptySheet").isEmpty(), is(true));
    }

    @Test
    void noDoubleRenderingOfSheetTitles() throws Exception {
        var spreadsheet = new Spreadsheet()
            .setSheets(
                List.of(
                    new Sheet().setProperties(
                        new SheetProperties()
                            .setTitle("Sheet {{ x }}")
                            .setGridProperties(new GridProperties().setRowCount(5).setColumnCount(2))
                    )
                )
            );

        var batchResponse = new BatchGetValuesResponse()
            .setValueRanges(
                List.of(
                    new ValueRange().setRange("Sheet {{ x }}!R1C1:R5C2").setValues(null)
                )
            );

        var sheets = mockSheets(spreadsheet, batchResponse);

        var task = Mockito.spy(
            Read.builder()
                .id(IdUtils.create())
                .type(Read.class.getName())
                .spreadsheetId(Property.ofValue("mockSpreadsheetId"))
                .selectedSheetsTitle(Property.ofExpression("{{ titles }}"))
                .fetch(Property.ofValue(true))
                .build()
        );

        Mockito.doReturn(sheets).when(task).connection(Mockito.any());

        var runContext = runContextFactory.of(Map.of("titles", List.of("Sheet {{ x }}")));
        var run = task.run(runContext);

        assertThat(run, notNullValue());
        assertThat(run.getSize(), is(0));
        assertThat(run.getRows().containsKey("Sheet {{ x }}"), is(true));
    }

    private Sheets mockSheets(Spreadsheet spreadsheet, BatchGetValuesResponse batchResponse) throws Exception {
        var sheets = Mockito.mock(Sheets.class);
        var spreadsheets = Mockito.mock(Sheets.Spreadsheets.class);
        var get = Mockito.mock(Sheets.Spreadsheets.Get.class);
        var values = Mockito.mock(Sheets.Spreadsheets.Values.class);
        var batchGet = Mockito.mock(Sheets.Spreadsheets.Values.BatchGet.class);

        Mockito.when(sheets.spreadsheets()).thenReturn(spreadsheets);
        Mockito.when(spreadsheets.get(Mockito.anyString())).thenReturn(get);
        Mockito.when(get.execute()).thenReturn(spreadsheet);

        Mockito.when(spreadsheets.values()).thenReturn(values);
        Mockito.when(values.batchGet(Mockito.anyString())).thenReturn(batchGet);
        Mockito.when(batchGet.setRanges(Mockito.anyList())).thenReturn(batchGet);
        Mockito.when(batchGet.set(Mockito.anyString(), Mockito.any())).thenReturn(batchGet);
        Mockito.when(batchGet.execute()).thenReturn(batchResponse);

        return sheets;
    }
}
