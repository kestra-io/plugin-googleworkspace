package io.kestra.plugin.googleworkspace.sheets;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.googleworkspace.UtilsTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@DisabledIf(
    value = "isServiceAccountNotExists",
    disabledReason = "Disabled for CI/CD"
)
class SheetModifiedTriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static String serviceAccount;

    @BeforeAll
    static void intiAccount() throws Exception {
        serviceAccount = UtilsTest.serviceAccount();
    }

    @Test
    void buildTrigger() throws Exception {
        RunContext runContext = runContextFactory.of();

        SheetModifiedTrigger trigger = SheetModifiedTrigger.builder()
            .id("test")
            .serviceAccount(serviceAccount)
            .spreadsheetId(Property.ofValue("dummy"))
            .range(Property.ofValue("Sheet1!A1:B2"))
            .build();

        assertThat(trigger.getId(), is("test"));
    }

    private static boolean isServiceAccountNotExists() {
        return UtilsTest.class
            .getClassLoader()
            .getResource(".gcp-service-account.json") == null;
    }
}


