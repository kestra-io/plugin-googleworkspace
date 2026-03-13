# Kestra Google Workspace Plugin

## What

Harness Google Workspace APIs within Kestra data orchestration. Exposes 25 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Google Workspace, allowing orchestration of Google Workspace-based operations as part of data pipelines and automation workflows.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `googleworkspace`

### Key Plugin Classes

- `io.kestra.plugin.googleworkspace.calendar.DeleteEvent`
- `io.kestra.plugin.googleworkspace.calendar.EventCreatedTrigger`
- `io.kestra.plugin.googleworkspace.calendar.GetEvent`
- `io.kestra.plugin.googleworkspace.calendar.InsertEvent`
- `io.kestra.plugin.googleworkspace.calendar.ListEvents`
- `io.kestra.plugin.googleworkspace.calendar.UpdateEvent`
- `io.kestra.plugin.googleworkspace.chat.GoogleChatExecution`
- `io.kestra.plugin.googleworkspace.chat.GoogleChatIncomingWebhook`
- `io.kestra.plugin.googleworkspace.drive.Create`
- `io.kestra.plugin.googleworkspace.drive.Delete`
- `io.kestra.plugin.googleworkspace.drive.Download`
- `io.kestra.plugin.googleworkspace.drive.Export`
- `io.kestra.plugin.googleworkspace.drive.FileCreatedTrigger`
- `io.kestra.plugin.googleworkspace.drive.List`
- `io.kestra.plugin.googleworkspace.drive.Upload`
- `io.kestra.plugin.googleworkspace.mail.Get`
- `io.kestra.plugin.googleworkspace.mail.List`
- `io.kestra.plugin.googleworkspace.mail.MailReceivedTrigger`
- `io.kestra.plugin.googleworkspace.mail.Send`
- `io.kestra.plugin.googleworkspace.sheets.CreateSpreadsheet`
- `io.kestra.plugin.googleworkspace.sheets.DeleteSpreadsheet`
- `io.kestra.plugin.googleworkspace.sheets.Load`
- `io.kestra.plugin.googleworkspace.sheets.Read`
- `io.kestra.plugin.googleworkspace.sheets.ReadRange`
- `io.kestra.plugin.googleworkspace.sheets.SheetModifiedTrigger`

### Project Structure

```
plugin-googleworkspace/
├── src/main/java/io/kestra/plugin/googleworkspace/sheets/
├── src/test/java/io/kestra/plugin/googleworkspace/sheets/
├── build.gradle
└── README.md
```

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
