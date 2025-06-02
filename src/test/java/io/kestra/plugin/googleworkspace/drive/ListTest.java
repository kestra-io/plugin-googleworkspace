package io.kestra.plugin.googleworkspace.drive;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.util.Map;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ListTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        List task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .query(Property.ofValue("'1YgHpphjepA8gAme1J04ftxVf7j80XABU' in parents"))
            .serviceAccount(Property.ofValue(UtilsTest.serviceAccount()))
            .build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(151));
    }
}
