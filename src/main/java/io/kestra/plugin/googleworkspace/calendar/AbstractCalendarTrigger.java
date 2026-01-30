package io.kestra.plugin.googleworkspace.calendar;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.calendar.Calendar;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.googleworkspace.GcpInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCalendarTrigger extends AbstractTrigger implements GcpInterface {

    private static final String APPLICATION_NAME = "Kestra";

    @Schema(
        title = "Service account key",
        description = "Google service account JSON with Calendar API scope. Share each target calendar with this account email; if omitted, Application Default Credentials are used."
    )
    protected Property<String> serviceAccount;

    protected Calendar connection(RunContext runContext) throws Exception {
        GoogleCredentials credentials;

        if (this.serviceAccount != null) {
            String rServiceAccount = runContext.render(this.serviceAccount)
                .as(String.class)
                .orElseThrow();

            credentials = GoogleCredentials.fromStream(
                new ByteArrayInputStream(rServiceAccount.getBytes(StandardCharsets.UTF_8))
            );
        } else {
            credentials = GoogleCredentials.getApplicationDefault();
        }

        var rScopes = runContext.render(this.getScopes()).asList(String.class);
        credentials = credentials.createScoped(rScopes);

        return new Calendar.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName(APPLICATION_NAME)
            .build();
    }
}
