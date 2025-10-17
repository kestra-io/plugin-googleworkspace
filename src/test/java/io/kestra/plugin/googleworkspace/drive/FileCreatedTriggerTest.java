package io.kestra.plugin.googleworkspace.drive;

import io.kestra.core.models.property.Property;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class FileCreatedTriggerTest {

    private static final String MOCK_SERVICE_ACCOUNT = "{\"type\":\"service_account\",\"project_id\":\"test\",\"private_key_id\":\"test\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\ntest\\n-----END PRIVATE KEY-----\\n\",\"client_email\":\"test@test.iam.gserviceaccount.com\",\"client_id\":\"test\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\"}";

    @Test
    void testTriggerCreationWithBasicProperties() throws Exception {
        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
            .id("test-trigger")
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(MOCK_SERVICE_ACCOUNT)
            .folderId(Property.ofValue("test-folder-id"))
            .interval(Duration.ofMinutes(5))
            .build();

        // Test that all properties are set correctly
        assertEquals("test-trigger", trigger.getId());
        assertEquals(FileCreatedTrigger.class.getName(), trigger.getType());
        assertEquals(Property.ofValue("test-folder-id"), trigger.getFolderId());
        assertEquals(Duration.ofMinutes(5), trigger.getInterval());
        assertNotNull(trigger.getServiceAccount());
    }

    @Test
    void testTriggerWithMimeTypes() throws Exception {
        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
            .id("test-trigger-mime")
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(MOCK_SERVICE_ACCOUNT)
            .folderId(Property.ofValue("test-folder-id"))
            .mimeTypes(Property.ofValue(List.of("application/pdf", "text/plain")))
            .interval(Duration.ofMinutes(5))
            .build();

        // Test that MIME types are set correctly
        assertEquals(Property.ofValue(List.of("application/pdf", "text/plain")), trigger.getMimeTypes());
    }

    @Test
    void testTriggerWithOwnerEmail() throws Exception {
        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
            .id("test-trigger-owner")
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(MOCK_SERVICE_ACCOUNT)
            .folderId(Property.ofValue("test-folder-id"))
            .ownerEmail(Property.ofValue("test@example.com"))
            .interval(Duration.ofMinutes(5))
            .build();

        // Test that owner email is set correctly
        assertEquals(Property.ofValue("test@example.com"), trigger.getOwnerEmail());
    }

    @Test
    void testTriggerWithoutFolder() throws Exception {
        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
            .id("test-trigger-no-folder")
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(MOCK_SERVICE_ACCOUNT)
            .interval(Duration.ofMinutes(5))
            .build();

        // Test that trigger can be created without folder ID
        assertNull(trigger.getFolderId());
        assertEquals(Duration.ofMinutes(5), trigger.getInterval());
    }

    @Test
    void testTriggerWithAllProperties() throws Exception {
        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
            .id("test-trigger-full")
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(MOCK_SERVICE_ACCOUNT)
            .folderId(Property.ofValue("test-folder-id"))
            .mimeTypes(Property.ofValue(List.of("application/pdf")))
            .ownerEmail(Property.ofValue("test@example.com"))
            .includeSubfolders(Property.ofValue(true))
            .maxFilesPerPoll(Property.ofValue(50))
            .interval(Duration.ofMinutes(10))
            .build();

        // Test that all properties are set correctly
        assertEquals("test-trigger-full", trigger.getId());
        assertEquals(Property.ofValue("test-folder-id"), trigger.getFolderId());
        assertEquals(Property.ofValue(List.of("application/pdf")), trigger.getMimeTypes());
        assertEquals(Property.ofValue("test@example.com"), trigger.getOwnerEmail());
        assertEquals(Property.ofValue(true), trigger.getIncludeSubfolders());
        assertEquals(Property.ofValue(50), trigger.getMaxFilesPerPoll());
        assertEquals(Duration.ofMinutes(10), trigger.getInterval());
        assertNotNull(trigger.getServiceAccount());
    }

    @Test
    void testTriggerDefaults() throws Exception {
        FileCreatedTrigger trigger = FileCreatedTrigger.builder()
            .id("test-trigger-defaults")
            .type(FileCreatedTrigger.class.getName())
            .serviceAccount(MOCK_SERVICE_ACCOUNT)
            .build();

        // Test default values
        assertEquals(Duration.ofHours(1), trigger.getInterval()); // Default from @Builder.Default
        assertEquals(Property.ofValue(100), trigger.getMaxFilesPerPoll()); // Default from @Builder.Default
        assertEquals(Property.ofValue(false), trigger.getIncludeSubfolders()); // Default from @Builder.Default
        assertNotNull(trigger.getScopes()); // Should have default scopes
    }
}
