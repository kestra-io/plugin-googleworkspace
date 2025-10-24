package io.kestra.plugin.googleworkspace.calendar;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.CalendarScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.googleworkspace.GcpInterface;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCalendarTrigger extends AbstractTrigger implements GcpInterface {
    private static final String APPLICATION_NAME = "Kestra";

    protected Property<String> serviceAccount;

    protected Calendar connection(RunContext runContext) throws Exception {
        String renderedServiceAccount = runContext.render(this.serviceAccount).as(String.class).orElseThrow();

        GoogleCredentials credentials = GoogleCredentials
            .fromStream(new ByteArrayInputStream(renderedServiceAccount.getBytes(StandardCharsets.UTF_8)))
            .createScoped(Collections.singletonList(CalendarScopes.CALENDAR));

        return new Calendar.Builder(
            GoogleNetHttpTransport.newTrustedTransport(),
            GsonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(credentials)
        )
            .setApplicationName(APPLICATION_NAME)
            .build();
    }
}