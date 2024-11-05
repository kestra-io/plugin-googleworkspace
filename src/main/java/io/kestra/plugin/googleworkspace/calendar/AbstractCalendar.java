package io.kestra.plugin.googleworkspace.calendar;

import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.CalendarScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.googleworkspace.AbstractTask;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractCalendar extends AbstractTask {
    @Builder.Default
    @PluginProperty(dynamic = true)
    protected List<String> scopes = List.of(CalendarScopes.CALENDAR);

    protected Calendar connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException, GeneralSecurityException {
        HttpCredentialsAdapter credentials = this.credentials(runContext);

        return new Calendar.Builder(this.netHttpTransport(), JSON_FACTORY, credentials)
            .setApplicationName("Kestra")
            .build();
    }
}
