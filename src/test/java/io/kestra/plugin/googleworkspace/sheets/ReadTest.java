package io.kestra.plugin.googleworkspace.sheets;

import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class ReadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        Read task = Read.builder()
            .id(ReadTest.class.getSimpleName())
            .type(ReadRange.class.getName())
            .spreadsheetId("1Dkd7W0OQo-wxz9rrORLP7YGSj6EBLEg73fiTdbJUIQE")
            .serviceAccount(UtilsTest.serviceAccount())
            .fetch(true)
            .build();

        Read.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getSize(), is(93));
        assertThat(((Map<String, Object>) run.getRows().get("Class Data").get(6)).get("Date"), is("7/11/2012"));
        assertThat(((Map<String, Object>) run.getRows().get("Class Data").get(6)).get("Date"), is("7/11/2012"));
        assertThat(((Map<String, Object>) run.getRows().get("Second One").get(0)).get("Formula"), is("Female"));
        assertThat(((Map<String, Object>) run.getRows().get("2 Tables").get(0)).get("Student Name"), is("Alexandra"));
        assertThat(((Map<String, Object>) run.getRows().get("2 Tables").get(0)).size(), is(5));
        assertThat(((Map<String, Object>) run.getRows().get("2 Tables").get(25)).get("Student Name"), is("Robert"));
        assertThat(((Map<String, Object>) run.getRows().get("2 Tables").get(25)).size(), is(2));
    }


    @SuppressWarnings("unchecked")
    @Test
    void selected() throws Exception {
        Read task = Read.builder()
            .id(ReadTest.class.getSimpleName())
            .type(ReadRange.class.getName())
            .spreadsheetId("1Dkd7W0OQo-wxz9rrORLP7YGSj6EBLEg73fiTdbJUIQE")
            .serviceAccount(UtilsTest.serviceAccount())
            .selectedSheetsTitle(List.of("Class Data"))
            .fetch(true)
            .build();

        Read.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getRows().size(), is(1));
        assertThat(run.getSize(), is(31));
        assertThat(((Map<String, Object>) run.getRows().get("Class Data").get(6)).get("Date"), is("7/11/2012"));
        assertThat(((Map<String, Object>) run.getRows().get("Class Data").get(6)).get("Date"), is("7/11/2012"));
    }
}
