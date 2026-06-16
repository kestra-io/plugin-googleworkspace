# How to use the Google Workspace plugin

Access Google Drive, Sheets, Calendar, Gmail, and Chat from Kestra flows using the authentication method appropriate for each service.

## Authentication

**Service account** (Drive, Sheets, Calendar): set `serviceAccount` to the JSON key content of a GCP service account with the appropriate Workspace API scope enabled. Store it in a [secret](https://kestra.io/docs/concepts/secret) and reference it with `{{ secret('GWS_SERVICE_ACCOUNT') }}`. Apply globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults) if all tasks in a flow target the same account.

**OAuth 2.0** (Gmail/Mail): set `clientId`, `clientSecret`, and `refreshToken` on mail tasks. These are obtained from a Google Cloud OAuth 2.0 client ID credential. Store all three in secrets.

**Incoming webhook** (Chat): `GoogleChatIncomingWebhook` and `GoogleChatExecution` use a webhook `url` with no additional auth. Create a webhook in the Google Chat space settings.

## Tasks

`drive` tasks handle file operations: `Upload` and `Download` move files between Kestra internal storage and Drive; `Create` creates folders; `Delete` removes files; `List` queries Drive contents; `Export` converts Google-native formats (Docs, Sheets) to a downloadable format. `FileCreatedTrigger` starts an execution when a new file appears in a Drive folder.

`sheets` tasks read and write spreadsheets: `Read` fetches all rows, `ReadRange` fetches a specific A1-notation range, `Load` writes rows to a sheet, `CreateSpreadsheet` and `DeleteSpreadsheet` manage spreadsheet lifecycle. Set `spreadsheetId` to the ID from the spreadsheet URL. `SheetModifiedTrigger` fires when a sheet is updated.

`calendar` tasks manage events: `InsertEvent` creates an event, `UpdateEvent` modifies it, `DeleteEvent` removes it, `GetEvent` reads it, and `ListEvents` queries a date range. Set `calendarId` to the calendar's email-style identifier. `EventCreatedTrigger` starts an execution when a new event is created.

`mail` tasks interact with Gmail: `Send` sends a message, `Get` reads a single message by ID, `List` queries the mailbox with a Gmail query string. `MailReceivedTrigger` polls for new messages matching a query and starts one execution per batch.

`chat` tasks send Workspace Chat messages: `GoogleChatIncomingWebhook` posts a message using `payload` (a JSON Chat card body — see the [Chat API message format](https://developers.google.com/chat/api/guides/message-formats/cards)). `GoogleChatExecution` sends a structured execution summary including status, duration, and an execution link, and is designed for use with a [Flow trigger](https://kestra.io/docs/workflow-components/triggers) in a monitoring namespace.
