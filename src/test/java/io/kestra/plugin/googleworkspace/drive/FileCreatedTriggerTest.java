package io.kestra.plugin.googleworkspace.drive;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.devskiller.friendly_id.FriendlyId;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.User;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@KestraTest
class FileCreatedTriggerTest {
    private static final Logger logger = LoggerFactory.getLogger(FileCreatedTriggerTest.class);
    private static final String SERVICE_ACCOUNT = "{\"type\":\"service_account\",\"project_id\":\"test\"}";
    private static final String FOLDER_QUERY_PREFIX = "mimeType = 'application/vnd.google-apps.folder'";
    private static final Pattern PARENT_ID = Pattern.compile("'([^']+)' in parents");

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
    void evaluate_defaultIncludeSubfolders_onlyDirectParent() throws Exception {
        var trigger = trigger("query-default")
            .folderId(Property.ofValue("FOLDER"))
            .build();

        var mockedDrive = mockDrive(Map.of(), 1000, List.of());
        evaluate(trigger, mockedDrive);

        assertThat(mockedDrive.q().getAllValues(), contains(matchesPattern("trashed = false and createdTime > '[^']+' and 'FOLDER' in parents")));
    }

    @Test
    void evaluate_includeSubfoldersFalse_onlyDirectParent() throws Exception {
        var trigger = trigger("query-false")
            .folderId(Property.ofValue("FOLDER"))
            .includeSubfolders(Property.ofValue(false))
            .build();

        var mockedDrive = mockDrive(Map.of(), 1000, List.of());
        evaluate(trigger, mockedDrive);

        assertThat(mockedDrive.q().getAllValues(), contains(matchesPattern("trashed = false and createdTime > '[^']+' and 'FOLDER' in parents")));
    }

    @Test
    void evaluate_includeSubfoldersTrue_orJoinsRootAndDescendants() throws Exception {
        var trigger = trigger("query-true")
            .folderId(Property.ofValue("folderA"))
            .includeSubfolders(Property.ofValue(true))
            .build();

        var mockedDrive = mockDrive(Map.of("folderA", List.of("folderB"), "folderB", List.of("folderC")), 1000, List.of());
        evaluate(trigger, mockedDrive);

        assertThat(mockedDrive.fileQueries(), contains(endsWith(" and ('folderA' in parents or 'folderB' in parents or 'folderC' in parents)")));
        assertThat(mockedDrive.folderQueries(), contains(folderQuery("folderA"), folderQuery("folderB"), folderQuery("folderC")));
    }

    @Test
    void evaluate_noFolderId_noParentFilter() throws Exception {
        var trigger = trigger("query-nofolder")
            .includeSubfolders(Property.ofValue(true))
            .build();

        var mockedDrive = mockDrive(Map.of(), 1000, List.of());
        evaluate(trigger, mockedDrive);

        assertThat(mockedDrive.q().getAllValues(), contains(matchesPattern("trashed = false and createdTime > '[^']+'")));
    }

    @Test
    void evaluate_subfoldersOnSeveralPages_followsNextPageToken() throws Exception {
        var trigger = trigger("query-pages")
            .folderId(Property.ofValue("root"))
            .includeSubfolders(Property.ofValue(true))
            .build();

        // Drive answers two folders per page, so the three subfolders of root take two pages
        var mockedDrive = mockDrive(Map.of("root", List.of("b", "c", "d")), 2, List.of());
        evaluate(trigger, mockedDrive);

        assertThat(mockedDrive.fileQueries(), contains(endsWith(" and ('root' in parents or 'b' in parents or 'c' in parents or 'd' in parents)")));
        assertThat(mockedDrive.folderQueries(), contains(folderQuery("root"), folderQuery("root"), folderQuery("b"), folderQuery("c"), folderQuery("d")));
        assertThat(mockedDrive.pageToken().getAllValues(), is(Arrays.asList(null, "2", null, null, null)));
        assertThat(mockedDrive.pageSize().getAllValues().subList(0, 5), contains(1000, 1000, 1000, 1000, 1000));
    }

    @Test
    void evaluate_folderWithTwoParents_listedOnce() throws Exception {
        var trigger = trigger("query-two-parents")
            .folderId(Property.ofValue("root"))
            .includeSubfolders(Property.ofValue(true))
            .build();

        // shared is in both b and c, so the walk reaches it twice
        var mockedDrive = mockDrive(
            Map.of(
                "root", List.of("b", "c"),
                "b", List.of("shared"),
                "c", List.of("shared"),
                "shared", List.of("leaf")
            ),
            1000,
            List.of()
        );
        evaluate(trigger, mockedDrive);

        assertThat(mockedDrive.fileQueries(), contains(endsWith(" and ('root' in parents or 'b' in parents or 'c' in parents or 'shared' in parents or 'leaf' in parents)")));
        assertThat(mockedDrive.folderQueries(), contains(folderQuery("root"), folderQuery("b"), folderQuery("c"), folderQuery("shared"), folderQuery("leaf")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void evaluate_includeSubfolders_triggersOnFileInSubfolder() throws Exception {
        var trigger = trigger("evaluate-subfolder")
            .folderId(Property.ofValue("root"))
            .includeSubfolders(Property.ofValue(true))
            .build();

        var file = newFile("file-in-sub", System.currentTimeMillis() - 10_000, "sub");
        var mockedDrive = mockDrive(Map.of("root", List.of("sub")), 1000, List.of(file));

        var execution = evaluate(trigger, mockedDrive);

        assertThat(mockedDrive.fileQueries(), contains(endsWith(" and ('root' in parents or 'sub' in parents)")));
        assertThat(execution.isPresent(), is(true));
        var files = (List<Map<String, Object>>) execution.get().getTrigger().getVariables().get("files");
        assertThat(files, hasSize(1));
        assertThat(files.getFirst().get("id"), is("file-in-sub"));
        assertThat(files.getFirst().get("parents"), is(List.of("sub")));
        assertThat(files.getFirst().get("kestraFileUri"), notNullValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    void evaluate_manySubfolders_queriesEachChunkAndMergesUpToMaxFilesPerPoll() throws Exception {
        var trigger = trigger("evaluate-chunks")
            .folderId(Property.ofValue("root"))
            .includeSubfolders(Property.ofValue(true))
            .maxFilesPerPoll(Property.ofValue(3))
            .build();

        // root and its 200 subfolders make 201 ids, queried in chunks of 50, 50, 50, 50 and 1
        var subfolderIds = IntStream.range(0, 200).mapToObj("folder-%03d"::formatted).toList();
        var now = System.currentTimeMillis();
        var files = List.of(
            newFile("in-chunk-1", now - 10_000, "folder-000"),
            newFile("in-chunk-5", now - 20_000, "folder-199"),
            newFile("in-chunks-1-and-4", now - 30_000, "folder-010", "folder-160"),
            newFile("in-chunk-3", now - 40_000, "folder-120")
        );
        var mockedDrive = mockDrive(Map.of("root", subfolderIds), 1000, files);

        var execution = evaluate(trigger, mockedDrive);

        // One file query per chunk, each folder id in exactly one of them
        var parentIdsPerQuery = mockedDrive.fileQueries().stream().map(FileCreatedTriggerTest::parentIds).toList();
        assertThat(parentIdsPerQuery.stream().map(List::size).toList(), contains(50, 50, 50, 50, 1));
        assertThat(parentIdsPerQuery.stream().flatMap(List::stream).toList(), is(Stream.concat(Stream.of("root"), subfolderIds.stream()).toList()));
        assertThat(mockedDrive.fileQueries().getLast(), endsWith(" and 'folder-199' in parents"));

        // The oldest files across all chunks, once each, and the newest one left out by maxFilesPerPoll
        assertThat(execution.isPresent(), is(true));
        var outputFiles = (List<Map<String, Object>>) execution.get().getTrigger().getVariables().get("files");
        assertThat(outputFiles.stream().map(outputFile -> (String) outputFile.get("id")).toList(), contains("in-chunk-3", "in-chunks-1-and-4", "in-chunk-5"));
    }

    private record MockedDrive(Drive drive, ArgumentCaptor<String> q, ArgumentCaptor<Integer> pageSize, ArgumentCaptor<String> pageToken) {
        List<String> fileQueries() {
            return q.getAllValues().stream().filter(query -> !query.startsWith(FOLDER_QUERY_PREFIX)).toList();
        }

        List<String> folderQueries() {
            return q.getAllValues().stream().filter(query -> query.startsWith(FOLDER_QUERY_PREFIX)).toList();
        }
    }

    // Answers folder queries with subfolders (paged) and file queries with the oldest matching files
    private MockedDrive mockDrive(Map<String, List<String>> subfolders, int folderPageSize, List<File> files) throws Exception {
        var q = ArgumentCaptor.forClass(String.class);
        var pageSize = ArgumentCaptor.forClass(Integer.class);
        var pageToken = ArgumentCaptor.forClass(String.class);

        var drive = mock(Drive.class);
        var driveFiles = mock(Drive.Files.class);
        var list = mock(Drive.Files.List.class, Mockito.RETURNS_SELF);

        when(drive.files()).thenReturn(driveFiles);
        when(driveFiles.list()).thenReturn(list);
        when(list.setQ(q.capture())).thenReturn(list);
        when(list.setPageSize(pageSize.capture())).thenReturn(list);
        when(list.setPageToken(pageToken.capture())).thenReturn(list);
        when(list.execute()).thenAnswer(invocation ->
        {
            var parentIds = parentIds(q.getValue());
            if (q.getValue().startsWith(FOLDER_QUERY_PREFIX)) {
                var children = subfolders.getOrDefault(parentIds.getFirst(), List.of());
                var from = pageToken.getValue() == null ? 0 : Integer.parseInt(pageToken.getValue());
                var to = Math.min(from + folderPageSize, children.size());
                return new FileList()
                    .setFiles(children.subList(from, to).stream().map(id -> new File().setId(id)).toList())
                    .setNextPageToken(to < children.size() ? String.valueOf(to) : null);
            }
            return new FileList().setFiles(
                files.stream()
                    .filter(file -> file.getParents().stream().anyMatch(parentIds::contains))
                    .sorted(Comparator.comparingLong(file -> file.getCreatedTime().getValue()))
                    .limit(pageSize.getValue())
                    .toList()
            );
        });

        var get = mock(Drive.Files.Get.class);
        when(driveFiles.get(anyString())).thenReturn(get);
        when(get.executeMediaAsInputStream()).thenAnswer(invocation -> new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)));

        return new MockedDrive(drive, q, pageSize, pageToken);
    }

    private static FileCreatedTrigger.FileCreatedTriggerBuilder<?, ?> trigger(String idPrefix) {
        return FileCreatedTrigger.builder()
            .id("%s-%s".formatted(idPrefix, IdUtils.create()))
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(Property.ofValue(SERVICE_ACCOUNT))
            .interval(Duration.ofMinutes(1));
    }

    private Optional<Execution> evaluate(FileCreatedTrigger trigger, MockedDrive mockedDrive) throws Exception {
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var spiedTrigger = spy(trigger);
        doReturn(mockedDrive.drive()).when(spiedTrigger).from(any());
        return spiedTrigger.evaluate(context.getKey(), context.getValue());
    }

    private static List<String> parentIds(String query) {
        return PARENT_ID.matcher(query).results().map(match -> match.group(1)).toList();
    }

    private static String folderQuery(String parentId) {
        return "mimeType = 'application/vnd.google-apps.folder' and '%s' in parents and trashed = false".formatted(parentId);
    }

    private File newFile(String id, long createdTime, String... parents) {
        return new File()
            .setId(id)
            .setName("%s.txt".formatted(id))
            .setCreatedTime(new DateTime(createdTime))
            .setParents(List.of(parents));
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