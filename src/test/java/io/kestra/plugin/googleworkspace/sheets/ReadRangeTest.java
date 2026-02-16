package io.kestra.plugin.googleworkspace.sheets;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ReadRangeTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @SuppressWarnings("unchecked")
    @Test
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
}
