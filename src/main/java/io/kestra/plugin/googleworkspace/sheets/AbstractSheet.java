package io.kestra.plugin.googleworkspace.sheets;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
public abstract class AbstractSheet extends AbstractTask {
    @Builder.Default
    protected Property<List<String>> scopes = Property.ofValue(List.of(SheetsScopes.SPREADSHEETS));

    protected Sheets connection(RunContext runContext) throws IllegalVariableEvaluationException, IOException, GeneralSecurityException {
        HttpCredentialsAdapter credentials = this.credentials(runContext);

        return new Sheets.Builder(this.netHttpTransport(), JSON_FACTORY, credentials)
            .setApplicationName("Kestra")
            .build();
    }
}
