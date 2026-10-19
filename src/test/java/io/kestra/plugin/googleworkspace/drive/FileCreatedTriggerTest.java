package io.kestra.plugin.googleworkspace.drive;

import java.time.Duration;
import java.util.*;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.devskiller.friendly_id.FriendlyId;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.User;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.FileList;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
class FileCreatedTriggerTest {
    private static final Logger logger = LoggerFactory.getLogger(FileCreatedTriggerTest.class);

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testTriggerWithMockFiles() throws Exception {

        String testFolderId = "test-folder-" + IdUtils.create();
        String mockServiceAccount = "{\"type\":\"service_account\",\"project_id\":\"test-project\"}";

        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
            .id(FileCreatedTriggerTest.class.getSimpleName() + IdUtils.create())
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(Property.ofValue(mockServiceAccount))
            .folderId(Property.ofValue(testFolderId))
            .interval(Duration.ofMinutes(1))
            .build();

        // Verify trigger configuration
        assertThat(trigger.getId(), notNullValue());
        assertThat(trigger.getType(), is(FileCreatedTrigger.class.getName()));
        assertThat(trigger.getInterval(), is(Duration.ofMinutes(1)));
        assertThat(trigger.getFolderId(), notNullValue());
        assertThat(trigger.getServiceAccount(), notNullValue());

        logger.info("Trigger configured successfully with ID: {}", trigger.getId());
    }

    @Test
    void testFileMetadataStructure() {
        // Test that we can create File objects with the expected structure
        String fileId = "test-file-" + FriendlyId.createFriendlyId();
        String fileName = "test-document.txt";
        String mimeType = "text/plain";
        Long fileSize = 2048L;
        String folderId = "test-folder-id";

        File mockFile = createMockFile(fileId, fileName, mimeType, fileSize, folderId);

        // Verify all expected fields are present
        assertThat(mockFile.getId(), is(fileId));
        assertThat(mockFile.getName(), is(fileName));
        assertThat(mockFile.getMimeType(), is(mimeType));
        assertThat(mockFile.getSize(), is(fileSize));
        assertThat(mockFile.getParents(), hasSize(1));
        assertThat(mockFile.getParents().get(0), is(folderId));
        assertThat(mockFile.getCreatedTime(), notNullValue());
        assertThat(mockFile.getModifiedTime(), notNullValue());
        assertThat(mockFile.getWebViewLink(), containsString(fileId));
        assertThat(mockFile.getOwners(), hasSize(1));
        assertThat(mockFile.getOwners().get(0).getEmailAddress(), notNullValue());
    }

    @Test
    void testMultipleFiles() {
        // Test creating multiple mock files
        List<File> files = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            String fileId = "file-" + i + "-" + FriendlyId.createFriendlyId();
            String fileName = "document-" + i + ".txt";
            files.add(createMockFile(fileId, fileName, "text/plain", 1024L, "test-folder"));
        }

        assertThat(files, hasSize(3));

        // Verify each file has unique ID and name
        Set<String> ids = new HashSet<>();
        Set<String> names = new HashSet<>();

        for (File file : files) {
            ids.add(file.getId());
            names.add(file.getName());
        }

        assertThat(ids, hasSize(3));
        assertThat(names, hasSize(3));
    }

    @Test
    void testFileWithDifferentMimeTypes() {
        // Test various mime types
        Map<String, String> mimeTypes = Map.of(
            "document.pdf", "application/pdf",
            "spreadsheet.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "image.png", "image/png",
            "video.mp4", "video/mp4"
        );

        for (Map.Entry<String, String> entry : mimeTypes.entrySet()) {
            File file = createMockFile(
                "file-" + IdUtils.create(),
                entry.getKey(),
                entry.getValue(),
                5000L,
                "test-folder"
            );

            assertThat(file.getName(), is(entry.getKey()));
            assertThat(file.getMimeType(), is(entry.getValue()));
        }
    }

    @Test
    void testRecentlyCreatedFile() {
        // Test file created very recently (within last minute)
        long now = System.currentTimeMillis();
        long thirtySecondsAgo = now - 30000;

        File recentFile = new File()
            .setId("recent-file-" + IdUtils.create())
            .setName("recent-document.txt")
            .setMimeType("text/plain")
            .setSize(1024L)
            .setCreatedTime(new DateTime(thirtySecondsAgo))
            .setModifiedTime(new DateTime(now))
            .setParents(List.of("test-folder"));

        // Verify the file's created time is recent
        long createdTimeMillis = recentFile.getCreatedTime().getValue();
        long ageInSeconds = (now - createdTimeMillis) / 1000;

        assertThat(ageInSeconds, lessThan(60L));
        assertThat(ageInSeconds, greaterThanOrEqualTo(0L));
    }

    @Test
    void testTriggerConfiguration() throws Exception {
        String folderId = "test-folder-" + IdUtils.create();
        String mockServiceAccount = "{\"type\":\"service_account\",\"project_id\":\"test-project\"}";

        // Test with default interval
        FileCreatedTrigger trigger1 = FileCreatedTrigger.builder()
            .id("trigger-1")
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(Property.ofValue(mockServiceAccount))
            .folderId(Property.ofValue(folderId))
            .interval(Duration.ofMinutes(5))
            .build();

        assertThat(trigger1.getInterval(), is(Duration.ofMinutes(5)));
        assertThat(trigger1.getFolderId(), notNullValue());

        // Test with shorter interval
        FileCreatedTrigger trigger2 = FileCreatedTrigger.builder()
            .id("trigger-2")
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(Property.ofValue(mockServiceAccount))
            .folderId(Property.ofValue(folderId))
            .interval(Duration.ofSeconds(30))
            .build();

        assertThat(trigger2.getInterval(), is(Duration.ofSeconds(30)));
        assertThat(trigger2.getFolderId(), notNullValue());
    }


    @Test
    void buildQuery_defaultIncludeSubfolders_onlyDirectParent() throws Exception {
        RunContext runContext = runContextFactory.of();
        String folderId = "FOLDER";

        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
            .id("query-default-" + IdUtils.create())
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(Property.ofValue("{\"type\":\"service_account\",\"project_id\":\"test\"}"))
            .folderId(Property.ofValue(folderId))
            .interval(Duration.ofMinutes(1))
            .build();

        Drive drive = mock(Drive.class);
        String query = trigger.buildQuery(runContext, "2026-01-01T00:00:00.000Z", drive);

        assertThat(query, containsString("'FOLDER' in parents"));
        assertThat(query, not(containsString(" or ")));
        assertThat(query, containsString("trashed = false"));
        assertThat(query, containsString("createdTime > '2026-01-01T00:00:00.000Z'"));
    }

    @Test
    void buildQuery_includeSubfoldersFalse_onlyDirectParent() throws Exception {
        RunContext runContext = runContextFactory.of();
        String folderId = "FOLDER";

        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
            .id("query-false-" + IdUtils.create())
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(Property.ofValue("{\"type\":\"service_account\",\"project_id\":\"test\"}"))
            .folderId(Property.ofValue(folderId))
            .includeSubfolders(Property.ofValue(false))
            .interval(Duration.ofMinutes(1))
            .build();

        Drive drive = mock(Drive.class);
        String query = trigger.buildQuery(runContext, "2026-01-01T00:00:00.000Z", drive);

        assertThat(query, containsString("'FOLDER' in parents"));
        // No OR-joined multi-folder parents clause for descendants
        assertThat(query, not(containsString(" or '")));
        assertThat(query, not(containsString("('FOLDER' in parents or")));
    }

    @Test
    void buildQuery_includeSubfoldersTrue_orJoinsRootAndDescendants() throws Exception {
        RunContext runContext = runContextFactory.of();
        String rootId = "folderA";
        String childId = "folderB";

        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
            .id("query-true-" + IdUtils.create())
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(Property.ofValue("{\"type\":\"service_account\",\"project_id\":\"test\"}"))
            .folderId(Property.ofValue(rootId))
            .includeSubfolders(Property.ofValue(true))
            .interval(Duration.ofMinutes(1))
            .build();

        Drive drive = mockDriveWithChildFolder(rootId, childId);
        String query = trigger.buildQuery(runContext, "2026-01-01T00:00:00.000Z", drive);

        assertThat(query, containsString("'folderA' in parents"));
        assertThat(query, containsString("'folderB' in parents"));
        assertThat(query, containsString(" or "));
        assertThat(query, containsString("('folderA' in parents or 'folderB' in parents)"));
        assertThat(query, containsString("trashed = false"));
    }

    @Test
    void buildQuery_noFolderId_noParentFilter() throws Exception {
        RunContext runContext = runContextFactory.of();

        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
            .id("query-nofolder-" + IdUtils.create())
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(Property.ofValue("{\"type\":\"service_account\",\"project_id\":\"test\"}"))
            .includeSubfolders(Property.ofValue(true))
            .interval(Duration.ofMinutes(1))
            .build();

        Drive drive = mock(Drive.class);
        String query = trigger.buildQuery(runContext, "2026-01-01T00:00:00.000Z", drive);

        assertThat(query, not(containsString("in parents")));
        assertThat(query, containsString("trashed = false"));
    }

    private Drive mockDriveWithChildFolder(String rootId, String childId) throws Exception {
        Drive drive = mock(Drive.class);
        Drive.Files files = mock(Drive.Files.class);
        Drive.Files.List list = mock(Drive.Files.List.class);

        when(drive.files()).thenReturn(files);
        when(files.list()).thenReturn(list);
        when(list.setQ(anyString())).thenReturn(list);
        when(list.setFields(anyString())).thenReturn(list);
        when(list.setPageToken(anyString())).thenReturn(list);

        File childFolder = new File().setId(childId).setMimeType("application/vnd.google-apps.folder");
        FileList rootChildren = new FileList().setFiles(List.of(childFolder)).setNextPageToken(null);
        // Child has no further folder children
        FileList emptyChildren = new FileList().setFiles(List.of()).setNextPageToken(null);

        when(list.execute()).thenReturn(rootChildren, emptyChildren);

        return drive;
    }

    private File createMockFile(String fileId, String fileName, String mimeType, Long size, String folderId) {
        long now = System.currentTimeMillis();

        return new File()
            .setId(fileId)
            .setName(fileName)
            .setMimeType(mimeType)
            .setSize(size)
            .setCreatedTime(new DateTime(now - 10000))
            .setModifiedTime(new DateTime(now - 5000))
            .setWebViewLink("https://drive.google.com/file/d/" + fileId + "/view")
            .setParents(List.of(folderId))
            .setOwners(
                List.of(
                    new User()
                        .setDisplayName("Test User")
                        .setEmailAddress("testuser@example.com")
                )
            );
    }
}