package io.kestra.plugin.googleworkspace;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Objects;

import com.google.common.io.CharStreams;

public class UtilsTest {
    public static String serviceAccount() throws Exception {
        File file = new File(
            Objects.requireNonNull(
                UtilsTest.class.getClassLoader()
                    .getResource(".gcp-service-account.json")
            )
                .toURI()
        );

        return CharStreams.toString(new InputStreamReader(new FileInputStream(file)));
    }
}
