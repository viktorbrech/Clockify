# A collection of scripts that comprise a simple Clockify integration with G-Suite (Google Calendar and Gmail).

## Files
* "Code.gs" is a Google Apps Script (Javascript) file, to be deployed inside a Google Sheet. It should be put on a time-interval trigger to read out sent email and calendar events into the sheet.
* "clockify_gsuite_integration.py" is a Python script that can be run locally. It reads out the information from the "Code.gs" sheet via http, and provides an interface to log sent email, meetings and filler "general time" to Clockify via the Clockify API.
* the CSV files in the "input_files" subdirectory provide the customer data used for logging activities to Clockify. See "input_files/instructions.md" for details.
* "enrich_inputs.py" is a simple script that adds Clockify project IDs to the "input_files/customer_project_tag.csv" file

## Dependencies
* "Code.gs" requires the Calendar API as a service, cf. https://developers.google.com/apps-script/guides/services/advanced
* "clockify_gsuite_integration.py" requires pandas, requests, and dotenv. It requires a Clockify API key and the ID of the Google Sheet provided in an .env file.

## Notes
This is an internal tool written for a particular team, so there are various design decision and hard-coded bits that reflect the specific context. It is early work in progress.