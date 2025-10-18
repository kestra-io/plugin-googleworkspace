package io.kestra.plugin.googleworkspace.drive;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.User;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
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
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
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
    private static final Logger logger = LoggerFactory.getLogger(FileCreatedTriggerTest.class);
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

    private Drive driveService;

    @Test
    void triggerFromFlow() throws Exception {
        List<String> createdFileIds = new ArrayList<>();

        try {
            // Mock flow listeners
            CountDownLatch queueCount = new CountDownLatch(1);

            // Scheduler
            DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, IdUtils.create(), 8, null);
            try (
                    AbstractScheduler scheduler = new JdbcScheduler(
                            this.applicationContext,
                            this.flowListeners
                    );
            ) {
                AtomicReference<Execution> last = new AtomicReference<>();

                // Wait for execution
                Flux<Execution> receive = TestsUtils.receive(executionQueue, executionWithError -> {
                    Execution execution = executionWithError.getLeft();
                    if (execution.getFlowId().equals("drive-file-listen")) {
                        last.set(execution);
                        queueCount.countDown();
                    }
                });

                // Create test files
                String fileName1 = "test-" + FriendlyId.createFriendlyId() + ".txt";
                String fileName2 = "test-" + FriendlyId.createFriendlyId() + ".txt";

                File fileId1 = createFile(testFolderId, fileName1, "text/plain", 1024L);
                File fileId2 = createFile(testFolderId, fileName2, "text/plain", 2048L);

                createdFileIds.add(String.valueOf(fileId1));
                createdFileIds.add(String.valueOf(fileId2));

                worker.run();
                scheduler.run();
                repositoryLoader.load(MAIN_TENANT, Objects.requireNonNull(FileCreatedTriggerTest.class.getClassLoader().getResource("flows/drive")));

                boolean await = queueCount.await(30, TimeUnit.SECONDS);
                try {
                    assertThat(await, is(true));
                } finally {
                    worker.shutdown();
                    receive.blockLast();
                }

                @SuppressWarnings("unchecked")
                List<FileCreatedTrigger.Output.FileMetadata> files =
                        (List<FileCreatedTrigger.Output.FileMetadata>) last.get().getTrigger().getVariables().get("files");

                assertThat(files, notNullValue());
                assertThat(files.size(), greaterThanOrEqualTo(1));
            }
        } finally {
            // Clean up created test files
            for (String fileId : createdFileIds) {
                try {
                    deleteFile(fileId);
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
        }
    }

    @Test
    void basicTrigger() throws Exception {
        List<String> createdFileIds = new ArrayList<>();

        try {
            FileCreatedTrigger trigger = FileCreatedTrigger.builder()
                    .id(FileCreatedTriggerTest.class.getSimpleName() + IdUtils.create())
                    .type(FileCreatedTrigger.class.getName())
                    .serviceAccount(UtilsTest.serviceAccount())
                    .folderId(Property.ofValue(testFolderId))
                    .interval(Duration.ofMinutes(1))
                    .build();

            // Create a test file
            String fileName = "test-" + FriendlyId.createFriendlyId() + ".txt";
            File fileId = createFile(testFolderId, fileName, "text/plain", 1024L);
            createdFileIds.add(String.valueOf(fileId));

            // Small delay to ensure file is fully created
            Thread.sleep(1000);

            Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context =
                    TestsUtils.mockTrigger(runContextFactory, trigger);
            Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

            assertThat(execution.isPresent(), is(true));

            @SuppressWarnings("unchecked")
            java.util.List<FileCreatedTrigger.Output.FileMetadata> files =
                    (java.util.List<FileCreatedTrigger.Output.FileMetadata>) execution.get().getTrigger().getVariables().get("files");

            assertThat(files.size(), greaterThanOrEqualTo(1));

            // Find our file
            Optional<FileCreatedTrigger.Output.FileMetadata> ourFile = files.stream()
                    .filter(f -> f.getName().equals(fileName))
                    .findFirst();

            assertThat(ourFile.isPresent(), is(true));
            assertThat(ourFile.get().getId(), is(fileId));
            assertThat(ourFile.get().getMimeType(), is("text/plain"));
            assertThat(ourFile.get().getKestraFileUri(), notNullValue());
        } finally {
            for (String fileId : createdFileIds) {
                try {
                    deleteFile(fileId);
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
        }
    }

    @Test
    void noNewFiles() throws Exception {
        List<String> createdFileIds = new ArrayList<>();

        try {
            FileCreatedTrigger trigger = FileCreatedTrigger.builder()
                    .id(FileCreatedTriggerTest.class.getSimpleName() + IdUtils.create())
                    .type(FileCreatedTrigger.class.getName())
                    .serviceAccount(UtilsTest.serviceAccount())
                    .folderId(Property.ofValue(testFolderId))
                    .interval(Duration.ofSeconds(1))
                    .build();

            Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context =
                    TestsUtils.mockTrigger(runContextFactory, trigger);
            Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

            assertThat(execution.isPresent(), is(false));
        } finally {
            for (String fileId : createdFileIds) {
                try {
                    deleteFile(fileId);
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
        }
    }

    private File createFile(String name, String id, String mimeType, Long size) {

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

    private Drive getDriveService() throws Exception {
        if (driveService == null) {
            GoogleCredentials credentials = GoogleCredentials.fromStream(
                    new ByteArrayInputStream(UtilsTest.serviceAccount().getBytes(StandardCharsets.UTF_8))
            ).createScoped(Collections.singleton("https://www.googleapis.com/auth/drive"));

            driveService = new Drive.Builder(
                    GoogleNetHttpTransport.newTrustedTransport(),
                    GsonFactory.getDefaultInstance(),
                    new HttpCredentialsAdapter(credentials)
            )
                    .setApplicationName("kestra test")
                    .build();
        }
        return driveService;
    }

    private void deleteFile(String fileId) throws Exception {
        try {
            Drive drive = getDriveService();
            drive.files().delete(fileId).execute();
            logger.debug("Deleted file: {}", fileId);

        } catch (Exception e ) {
            logger.warn("Failed to delete file {}: {}", fileId, e.getMessage());
        }
    }
}
