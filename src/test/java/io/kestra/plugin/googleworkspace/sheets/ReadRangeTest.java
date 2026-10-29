package io.kestra.plugin.googleworkspace.sheets;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.mockito.Mockito;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.ValueRange;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class ReadRangeTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @SuppressWarnings("unchecked")
    @Test
    @DisabledIf(
        value = "isServiceAccountNotExists",
        disabledReason = "Disabled without GCP credentials"
    )
    void rangeFetch() throws Exception {
        ReadRange task = ReadRange.builder()
            .id(ReadRangeTest.class.getSimpleName())
            .type(ReadRange.class.getName())
            .spreadsheetId(Property.ofValue("1Dkd7W0OQo-wxz9rrORLP7YGSj6EBLEg73fiTdbJUIQE"))
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .range(Property.ofValue("Class Data!A1:I"))
            .fetch(Property.ofValue(true))
            .build();

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getSize(), is(30));
        assertThat(((Map<String, Object>) run.getRows().get(0)).get("Date"), is("1/1/2012"));
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisabledIf(
        value = "isServiceAccountNotExists",
        disabledReason = "Disabled without GCP credentials"
    )
    void rangeStore() throws Exception {
        ReadRange task = ReadRange.builder()
            .id(ReadRangeTest.class.getSimpleName())
            .type(ReadRange.class.getName())
            .spreadsheetId(Property.ofValue("1Dkd7W0OQo-wxz9rrORLP7YGSj6EBLEg73fiTdbJUIQE"))
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .range(Property.ofValue("Second One!A1:I"))
            .build();

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getSize(), is(30));

        List<Object> result = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, run.getUri()))), result::add);

        assertThat(((Map<String, Object>) result.get(0)).get("Student Name"), is("Alexandra"));
        assertThat(((Map<String, Object>) result.get(0)).get("Formula"), is("Female"));
    }

    @Test
    void emptyRange() throws Exception {
        var sheets = Mockito.mock(Sheets.class);
        var spreadsheets = Mockito.mock(Sheets.Spreadsheets.class);
        var values = Mockito.mock(Sheets.Spreadsheets.Values.class);
        var get = Mockito.mock(Sheets.Spreadsheets.Values.Get.class);

        Mockito.when(sheets.spreadsheets()).thenReturn(spreadsheets);
        Mockito.when(spreadsheets.values()).thenReturn(values);
        Mockito.when(values.get(Mockito.anyString(), Mockito.any())).thenReturn(get);
        Mockito.when(get.set(Mockito.anyString(), Mockito.any())).thenReturn(get);

        var valueRange = new ValueRange()
            .setRange("EmptySheet!A1:Z50")
            .setValues(null);

        Mockito.when(get.execute()).thenReturn(valueRange);

        var task = Mockito.spy(
            ReadRange.builder()
                .id(IdUtils.create())
                .type(ReadRange.class.getName())
                .spreadsheetId(Property.ofValue("mockSpreadsheetId"))
                .range(Property.ofValue("EmptySheet!A1:Z50"))
                .fetch(Property.ofValue(true))
                .build()
        );

        Mockito.doReturn(sheets).when(task).connection(Mockito.any());

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run, notNullValue());
        assertThat(run.getSize(), is(0));
        assertThat(run.getRows().isEmpty(), is(true));
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource(".gcp-service-account.json") == null;
    }
}
