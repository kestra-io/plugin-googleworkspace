package io.kestra.plugin.googleworkspace;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.junit.jupiter.api.Assumptions;

import com.google.common.io.CharStreams;

public class UtilsTest {
    public static String serviceAccount() throws Exception {
        var resource = UtilsTest.class.getClassLoader().getResource(".gcp-service-account.json");
        // Fork PRs get no GOOGLE_SERVICE_ACCOUNT secret, so tests that need it are skipped instead of failing
        Assumptions.assumeTrue(resource != null, "Google service account not configured, skipping integration test");
        var file = new File(resource.toURI());

        return CharStreams.toString(new InputStreamReader(new FileInputStream(file)));
    }
}
