package io.kestra.plugin.googleworkspace.drive;

import com.google.api.client.util.DateTime;
import com.google.api.services.drive.model.User;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import com.devskiller.friendly_id.FriendlyId;
import com.google.api.services.drive.model.File;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.worker.DefaultWorker;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class FileCreatedTriggerTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListeners flowListeners;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kestra.tasks.googleworkspace.drive.test-folder-id}")
    private String testFolderId;

    private List<String> createdFileIds = new ArrayList<>();


    @BeforeEach
    void setup() {
        createdFileIds.clear();
    }

    @AfterEach
    void tearDown() {
        for (String fileId : createdFileIds) {

        }
    }

    @Test
    void triggerFromFlow() throws Exception {
        CountDownLatch queueCount = new CountDownLatch(1);

        DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, IdUtils.create(), 8, null);
        try(
                AbstractScheduler scheduler = new JdbcScheduler(
                        this.applicationContext,
                        this.flowListeners
                )
                ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            Flux<Execution>  receive = TestsUtils.receive(executionQueue, executionWithError -> {
                Execution execution = executionWithError.getLeft();
                if (execution.getFlowId().equals("drive-file-listen")) {
                    last.set(execution);
                    queueCount.countDown();
                }
            });

            String fileName1 = "test-" + FriendlyId.createFriendlyId() + ".txt";
            String fileName2 = "test-" + FriendlyId.createFriendlyId() + ".txt";
            File mockFile1 = createMockFiles(fileName1, testFolderId, "text/plain",  1024L);
            File mockFile2 = createMockFiles(fileName2, testFolderId, "text/plain", 2024L);

            createdFileIds.add(String.valueOf(mockFile1));
            createdFileIds.add(String.valueOf(mockFile2));

            worker.run();
            scheduler.run();
            repositoryLoader.load(MAIN_TENANT, Objects.requireNonNull(FileCreatedTriggerTest.class.getClassLoader().getResource("flows/drive")));

            boolean await = queueCount.await(20, TimeUnit.SECONDS);
            try {
                assertThat(await, is(true));
            } finally {
                worker.shutdown();
                receive.blockLast();
            }

            java.util.List<FileCreatedTrigger.Output.FileMetadata> files =
                    (java.util.List<FileCreatedTrigger.Output.FileMetadata>) last.get().getTrigger().getVariables().get("files");

            assertThat(files.size(), is(2));
            assertThat(files.getFirst().getName(), anyOf(is(fileName1), is(fileName2)));
        }
    }

    @Test
    void basicTrigger() throws Exception {
        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
                .id(FileCreatedTriggerTest.class.getSimpleName() + IdUtils.create())
                .type(FileCreatedTrigger.class.getName())
                .serviceAccount(UtilsTest.serviceAccount())
                .folderId(Property.ofValue(testFolderId))
                .interval(Duration.ofMinutes(1))
                .build();

        String fileName = "test-" + FriendlyId.createFriendlyId() + ".txt";
        File file = createMockFiles(fileName, testFolderId,"text/plain", 2048L);
        createdFileIds.add(String.valueOf(file));

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context =
                TestsUtils.mockTrigger(runContextFactory,trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        java.util.List<FileCreatedTrigger.Output.FileMetadata> files =
                (java.util.List<FileCreatedTrigger.Output.FileMetadata>) execution.get().getTrigger().getVariables().get("files");

        assertThat(files.size(), is(1));
        assertThat(files.getFirst().getId(), is(file));
        assertThat(files.getFirst().getName(), is(fileName));
        assertThat(files.getFirst().getMimeType(), is("text/plain"));
        assertThat(files.getFirst().getKestraFileUri(), notNullValue());
    }

    @Test
    void noNewFiles() throws Exception {
        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
                .id(FileCreatedTriggerTest.class.getSimpleName() + IdUtils.create())
                .type(FileCreatedTrigger.class.getName())
                .serviceAccount(UtilsTest.serviceAccount())
                .folderId(Property.ofValue(testFolderId))
                .interval(Duration.ofMinutes(1))
                .build();

        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context =
                TestsUtils.mockTrigger(runContextFactory,trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(false));
    }

    private File createMockFiles(String name, String id, String mimeType, Long size) {

        return new File()
                .setId(id)
                .setName(name)
                .setMimeType(mimeType)
                .setSize(size)
                .setCreatedTime(new DateTime(System.currentTimeMillis() - 10000))
                .setModifiedTime(new DateTime(System.currentTimeMillis() - 5000))
                .setWebViewLink("https://drive.google.com/file/d/" + id + "/view")
                .setParents(List.of("test-folder-id"))
                .setOwners(List.of(new User().setDisplayName("Test User").setEmailAddress("test@example.com")));
    }
}
