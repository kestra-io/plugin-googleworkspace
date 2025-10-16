### Authentication

All tasks must be authenticated for the Google Cloud Platform. You can do it in multiple ways:

- By setting the task `serviceAccount` property that must contain the service account JSON content. It can be handy to set this property globally by using [plugin defaults](../../docs/configuration/index.md#plugin-defaults).
- By setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable on the server running Kestra. It must point to an application credentials file. **Warning:** It must be the same on all worker nodes and can cause some security concerns.

### Google Sheets Trigger: SheetModifiedTrigger

Detect changes to a Google Sheet and emit a diff for a given range.

```yaml
id: sheet_modified_example
namespace: company.team

tasks:
  - id: log
    type: io.kestra.plugin.core.log.Log
    message: "Change on {{ trigger.sheetName }}: {{ json(trigger.diff) }}"

triggers:
  - id: on_change
    type: io.kestra.plugin.googleworkspace.sheets.SheetModifiedTrigger
    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
    spreadsheetId: "your-spreadsheet-id"
    range: "Sheet1!A1:D100"
    interval: PT2M
```