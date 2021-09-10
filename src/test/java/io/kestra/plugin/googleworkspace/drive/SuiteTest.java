package io.kestra.plugin.googleworkspace.drive;

import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.googleworkspace.UtilsTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
class SuiteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        URI source = storageInterface.put(
            new URI("/" + IdUtils.create()),
            new FileInputStream(new File(Objects.requireNonNull(SuiteTest.class.getClassLoader()
                    .getResource("examples/addresses.csv"))
                .toURI()))
        );

        Upload upload = Upload.builder()
            .id(SuiteTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(source.toString())
            .to("1HuxzpLt1b0111MuKMgy8wAv-m9Myc1E_")
            .name(IdUtils.create())
            .contentType("text/csv")
            .mimeType("application/vnd.google-apps.spreadsheet")
            .serviceAccount(UtilsTest.serviceAccount())
            .build();

        Upload.Output uploadRun = upload.run(TestsUtils.mockRunContext(runContextFactory, upload, Map.of()));

        assertThat(uploadRun.getFile().getSize(), is(1024L));

        Export export = Export.builder()
            .id(SuiteTest.class.getSimpleName())
            .type(Export.class.getName())
            .fileId(uploadRun.getFile().getId())
            .contentType("text/csv")
            .serviceAccount(UtilsTest.serviceAccount())
            .build();

        Export.Output exportRun = export.run(TestsUtils.mockRunContext(runContextFactory, upload, Map.of()));

        InputStream get = storageInterface.get(exportRun.getUri());
        String getContent = CharStreams.toString(new InputStreamReader(get));

        assertThat(getContent, containsString("John,Doe"));
        assertThat(getContent, containsString("Desert City,CO,123"));

        Delete delete = Delete.builder()
            .id(SuiteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .fileId(uploadRun.getFile().getId())
            .serviceAccount(UtilsTest.serviceAccount())
            .build();

        Delete.Output deleteRun = delete.run(TestsUtils.mockRunContext(runContextFactory, upload, Map.of()));

        assertThat(deleteRun.getFileId(), is(exportRun.getFile().getId()));

        assertThrows(Exception.class, () -> {
            export.run(TestsUtils.mockRunContext(runContextFactory, upload, Map.of()));
        });
    }

    @Test
    void binary() throws Exception {
        File file = new File(Objects.requireNonNull(SuiteTest.class.getClassLoader()
                .getResource("examples/addresses.zip"))
            .toURI());

        URI source = storageInterface.put(
            new URI("/" + IdUtils.create()),
            new FileInputStream(file)
        );

        Upload upload = Upload.builder()
            .id(SuiteTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(source.toString())
            .to("1HuxzpLt1b0111MuKMgy8wAv-m9Myc1E_")
            .name(IdUtils.create())
            .contentType("application/zip")
            .serviceAccount(UtilsTest.serviceAccount())
            .build();

        Upload.Output uploadRun = upload.run(TestsUtils.mockRunContext(runContextFactory, upload, Map.of()));

        assertThat(uploadRun.getFile().getSize(), is(390L));

        Download download = Download.builder()
            .id(SuiteTest.class.getSimpleName())
            .type(Export.class.getName())
            .fileId(uploadRun.getFile().getId())
            .serviceAccount(UtilsTest.serviceAccount())
            .build();

        Download.Output downloadRun = download.run(TestsUtils.mockRunContext(runContextFactory, upload, Map.of()));
        InputStream get = storageInterface.get(downloadRun.getUri());

        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(file))))
        );

        Delete delete = Delete.builder()
            .id(SuiteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .fileId(uploadRun.getFile().getId())
            .serviceAccount(UtilsTest.serviceAccount())
            .build();

        Delete.Output deleteRun = delete.run(TestsUtils.mockRunContext(runContextFactory, upload, Map.of()));

        assertThat(deleteRun.getFileId(), is(downloadRun.getFile().getId()));
    }
}
