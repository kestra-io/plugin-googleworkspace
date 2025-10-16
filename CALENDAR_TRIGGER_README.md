# Google Calendar Event Trigger

## Overview

The `EventCreatedTrigger` monitors Google Calendar for newly created events and triggers workflow executions when new events are detected.

## Features

- Monitor single or multiple calendars
- Filter events by organizer, keywords, or status  
- Configurable polling intervals (minimum 1 minute)
- Access to complete event data
- Automatic duplicate prevention

## Configuration

### Required Parameters

- `serviceAccount`: Google Cloud service account JSON key with Calendar API access

### You can also set:
- `calendarId`: Watch one specific calendar (like "primary" for your main calendar)
- `calendarIds`: Watch multiple calendars (don't use both this and calendarId)
- `interval`: How often to check for new events (default: 5 minutes, minimum: 1 minute)
- `q`: Only trigger for events containing certain words
- `organizerEmail`: Only trigger for events created by a specific person
- `eventStatus`: Only trigger for events with specific status (confirmed, tentative, cancelled)
- `maxEventsPerPoll`: Don't process more than this many events at once (default: 100, max: 2500)

## Examples

### Simple example: Watch your main calendar

This will log a message every time someone creates a new event in your primary calendar:

```yaml
id: calendar_event_monitor
namespace: company.team

tasks:
  - id: log_new_event
    type: io.kestra.plugin.core.log.Log
    message: "New event: {{ trigger.summary }} at {{ trigger.start.dateTime }}"

triggers:
  - id: watch_primary_calendar
    type: io.kestra.plugin.googleworkspace.calendar.EventCreatedTrigger
    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
    calendarId: primary
    interval: PT5M
```

### More complex example: Multiple calendars with filtering

This watches team calendars and sends for example a Slack notification when someone creates a meeting with "manager@company.com" as the organizer:

```yaml
id: team_meeting_monitor
namespace: company.team

tasks:
  - id: notify_slack
    type: io.kestra.plugin.notifications.slack.SlackIncomingWebhook
    url: "{{ secret('SLACK_WEBHOOK') }}"
    payload: |
      {
        "text": "New meeting: {{ trigger.summary }}\nOrganizer: {{ trigger.organizer.displayName }}\nTime: {{ trigger.start.dateTime }}"
      }

triggers:
  - id: watch_team_meetings
    type: io.kestra.plugin.googleworkspace.calendar.EventCreatedTrigger
    serviceAccount: "{{ secret('GCP_SERVICE_ACCOUNT_JSON') }}"
    calendarIds:
      - "team-calendar@company.com"
      - "project-calendar@company.com"
    q: "meeting"
    organizerEmail: "manager@company.com"
    eventStatus: "confirmed"
    interval: PT10M
```

## What data you get

When the trigger fires, your workflow gets all this information about the event:

```json
{
  "id": "event-id-123",
  "summary": "Team Standup",
  "description": "Daily team standup meeting",
  "location": "Conference Room A",
  "status": "confirmed",
  "htmlLink": "https://calendar.google.com/event?eid=...",
  "created": "2025-10-13T10:30:00Z",
  "updated": "2025-10-13T10:35:00Z",
  "start": {
    "dateTime": "2025-10-14T09:00:00Z",
    "timeZone": "UTC"
  },
  "end": {
    "dateTime": "2025-10-14T09:30:00Z", 
    "timeZone": "UTC"
  },
  "organizer": {
    "email": "organizer@company.com",
    "displayName": "Meeting Organizer"
  },
  "visibility": "default",
  "eventType": "default"
}
```

You can access this data in your workflows using `{{ trigger.summary }}`, `{{ trigger.organizer.email }}`, etc.

## Getting started

Before you can use this trigger, you need to set up Google Cloud:

1. **Create a Google Cloud project** and enable the Calendar API
2. **Create a service account** with calendar permissions
3. **Download the service account JSON key** and add it to Kestra as a secret

The service account needs the `https://www.googleapis.com/auth/calendar` scope and permission to read the calendars you want to monitor.

## Important details

### Performance tips
- Checking calendars too frequently uses up your Google API quota faster
- Each calendar you monitor requires a separate API call
- Use filters (keywords, organizer, status) to reduce processing load
- The `maxEventsPerPoll` setting prevents overwhelming your system

### Error handling
The trigger is designed to be reliable:
- If you configure it wrong, you'll get a clear error message
- If Google API is temporarily down, it logs the error and tries again later
- If one calendar fails, it continues checking other calendars
- It automatically respects Google's rate limits

### Authentication notes
- Use service account authentication (not OAuth user tokens)
- Store your service account JSON key as a Kestra secret, never in plain text
- The service account needs read access to any calendars you want to monitor
- You can share specific calendars with your service account's email address