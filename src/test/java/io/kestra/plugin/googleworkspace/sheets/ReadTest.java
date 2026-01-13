package io.kestra.plugin.googleworkspace.sheets;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.RetryUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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
}
