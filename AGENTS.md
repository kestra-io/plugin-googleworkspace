# Kestra Google Workspace Plugin

## What

- Provides plugin components under `io.kestra.plugin.googleworkspace`.
- Includes classes such as `Delete`, `Upload`, `List`, `Create`.

## Why

- This plugin integrates Kestra with Calendar.
- It provides tasks that create, update, delete, and watch Google Calendar events.

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

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
