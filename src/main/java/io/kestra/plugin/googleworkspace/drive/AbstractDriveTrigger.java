package io.kestra.plugin.googleworkspace.drive;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.runners.RunContext;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public abstract class AbstractDriveTrigger extends AbstractTrigger {
    private static final String APPLICATION_NAME = "Kestra";
    private static final String DRIVE_SCOPE = "https://www.googleapis.com/auth/drive";


    public static Drive from(RunContext runContext, String serviceAccountJson) throws Exception {
        String renderedServiceAccount = runContext.render(serviceAccountJson);

        GoogleCredentials credentials = GoogleCredentials
            .fromStream(new ByteArrayInputStream(renderedServiceAccount.getBytes(StandardCharsets.UTF_8)))
            .createScoped(Collections.singleton(DRIVE_SCOPE));

        return new Drive.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName(APPLICATION_NAME)
            .build();
    }
}
