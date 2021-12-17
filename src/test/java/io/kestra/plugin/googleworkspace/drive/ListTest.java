package io.kestra.plugin.googleworkspace.drive;

import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.util.Map;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class ListTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        List task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .query("'1YgHpphjepA8gAme1J04ftxVf7j80XABU' in parents")
            .serviceAccount(UtilsTest.serviceAccount())
            .build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(151));
    }
}
