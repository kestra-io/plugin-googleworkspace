### Authentication

All tasks must be authenticated for the Google Cloud Platform. You can do it in multiple ways:

- By setting the task `serviceAccount` property that must contain the service account JSON content. It can be handy to set this property globally by using [task defaults](../../docs/administrator-guide/configuration/others/#kestra-tasks-defaults).
- By setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable on the server running Kestra. It must point to an application credentials file. **Warning:** It must be the same on all worker nodes and can cause some security concerns.
