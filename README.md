# Databricks/Slack integration applications

This is a collection of Python applications and integration code designed to connect Databricks with Slack. The repo provides two example Slack bot implementations:

* `genie-slack-app` for exposing Databricks Genie rooms
* `endpoint-slack-app` for interfacing with a Databricks Model Serving endpoint

 The bots are deployed as Databricks Apps and can send user requests from Slack into a Databricks workspace, handle responses, and post results back into Slack conversations. The purpose is to demonstrate how teams can leverage Slack as a conversational interface for querying data, triggering Databricks workflows, or integrating Databricks functionality directly into Slack interactions.

 ## Setup

1. Create a new "from scratch" [Slack API app](https://api.slack.com/apps)
2. In the OAuth & Permissions section add the following scopes:
    * app_mentions:read
    * chat:write
    * im:history
    * im:read
    * im:write
3. Enable Socket Mode and generate an app-level token with scope `connections:write`
4. In Event Subscription subscribe to the following bot events:
    * app_mention
    * message.im
5. Install the app (Install App -> Install to Workspace) and authorize it
6. Enable Messages Tab (in App Home -> Show Tabs)

Deploy the `genie-slack-app` or `endpoint-slack-app` as a Databricks app and edit the `app.yaml` with the correct token settings.
Give permissions to the app principal to access the relevant resources (Genie/Warehouse access, Endpoint query permissions etc.)

## Credits

`genie-slack-app` originally created by [Ambarish Dongaonkar](https://github.com/adgitdemo/ad_databricks)

