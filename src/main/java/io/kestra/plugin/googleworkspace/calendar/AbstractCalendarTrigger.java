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
import jakarta.validation.constraints.NotNull;
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
        title = "The Google Cloud service account key",
        description = "Service account JSON key with access to Google Calendar API. " +
            "The service account must have read access to the calendars you want to monitor. " +
            "Share calendars with the service account's email address (found in the JSON key)."
    )
    @NotNull
    protected Property<String> serviceAccount;

    protected Calendar connection(RunContext runContext) throws Exception {
        String rServiceAccount = runContext.render(this.serviceAccount).as(String.class).orElseThrow();
        var rScopes = runContext.render(this.getScopes()).asList(String.class);

        GoogleCredentials credentials = GoogleCredentials
            .fromStream(new ByteArrayInputStream(renderedServiceAccount.getBytes(StandardCharsets.UTF_8)))
            .createScoped(renderedScopes);

        return new Calendar.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName(APPLICATION_NAME)
            .build();
    }
}