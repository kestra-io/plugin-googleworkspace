package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.sheets.v4.Sheets;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.runners.RunContext;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSheetTrigger extends AbstractTrigger {
    private static final String APPLICATION_NAME = "Kestra";

    public static Sheets sheetsFrom(RunContext runContext, String serviceAccountJson, List<String> scopes) throws Exception {
        String renderedServiceAccount = runContext.render(serviceAccountJson);

        GoogleCredentials credentials = GoogleCredentials
            .fromStream(new ByteArrayInputStream(renderedServiceAccount.getBytes(StandardCharsets.UTF_8)))
            .createScoped(scopes);

        return new Sheets.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName(APPLICATION_NAME)
            .build();
    }

    public static Drive driveFrom(RunContext runContext, String serviceAccountJson, List<String> scopes) throws Exception {
        String renderedServiceAccount = runContext.render(serviceAccountJson);

        GoogleCredentials credentials = GoogleCredentials
            .fromStream(new ByteArrayInputStream(renderedServiceAccount.getBytes(StandardCharsets.UTF_8)))
            .createScoped(scopes);

        return new Drive.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName(APPLICATION_NAME)
            .build();
    }
}


