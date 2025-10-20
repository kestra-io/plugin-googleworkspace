package io.kestra.plugin.googleworkspace;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.CharStreams;
import io.kestra.core.serializers.JacksonMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;

public class UtilsTest {
    public static String serviceAccount() throws Exception {
        File file = new File(Objects.requireNonNull(UtilsTest.class.getClassLoader()
                .getResource(".gcp-service-account.json"))
            .toURI());

        return CharStreams.toString(new InputStreamReader(new FileInputStream(file)));
    }
    
    public static String oauthClientId() throws Exception {
        return getOAuthCredential("client_id");
    }
    
    public static String oauthClientSecret() throws Exception {
        return getOAuthCredential("client_secret");
    }
    
    public static String oauthRefreshToken() throws Exception {
        return getOAuthCredential("refresh_token");
    }
    
    public static String getOAuthCredential(String key) throws Exception {
        File file = new File(Objects.requireNonNull(UtilsTest.class.getClassLoader()
                .getResource(".gmail-oauth.json"))
            .toURI());
            
        String content = CharStreams.toString(new InputStreamReader(new FileInputStream(file)));
        Map<String, String> credentials = JacksonMapper.ofJson().readValue(content, new TypeReference<>() {});
        return credentials.get(key);
    }
}
