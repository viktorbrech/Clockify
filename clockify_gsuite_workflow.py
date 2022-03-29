######
# Imports
######

import requests
from datetime import datetime, timezone
import re

from hubspot import HubSpot
from hubspot.crm.contacts import ApiException

######
# Configuration
######

api_key = "OLD_Mzk3MTE2Y2EtODA0Yy00MDJiLThiODgtNzBjZDIwMWNjN2Ez"
sheet_id = "1PbmgdPUSbiudTL82MKw-02jSTmfvjZJNOqx5DJTI3hU"
hs_token = "HUBDB_ONLY_pat-na1-01d5e58a-3cf8-4d82-b4eb-c962ee0bce93"

headers = {'x-api-key': api_key}
sheet_base_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet="

r = requests.get('https://hubspot.clockify.me/api/v1/user', headers=headers)
context = r.json()
my_id = context["id"]
workspace = context["activeWorkspace"]

min_adjusted_meeting_length = 0.5 # fraction
max_meeting_start_delay = 0.33 # fraction

max_email_minutes = 15 # minutes
min_email_minutes = 5 # minutes
max_email_overlap = 3 # minutes, should be smaller than min_email_minutes

common_projects = {
    "general_time": "abc"
}

common_tags = {
    "meeting": "6172fd18d19f7568cf220734",
    "email": "6172fd18d19f7568cf2207ba",
    "investigate": "6172fd18d19f7568cf2207de",
    "post_call": "6172fd18d19f7568cf220758",
    "prep_call": "6172fd18d19f7568cf2207ef",
    "day_prep": "6172fd18d19f7568cf220817",
    "weekly_catchup": "6172fd1dd19f7568cf220c38",
    "lunch": "6172fd1ad19f7568cf2209cd"
}

######
# Utility functions
######

def isostr_to_ms(iso_str):
    if not iso_str:
        return int(datetime.now().timestamp() * 1000)
    else:
        assert iso_str[-1] == "Z"
        # use fromisoformat with Python 3.7+
        # assert datetime.strptime(iso_str[:-1] + "+00:00", "%Y-%m-%dT%H:%M:%S%z") == datetime.fromisoformat(iso_str[:-1] + "+00:00")
        # return int(1000 * datetime.fromisoformat(iso_str[:-1] + "+00:00").timestamp())
        
        # crazy workaround for Python 3.6, see https://stackoverflow.com/questions/30999230/how-to-parse-timezone-with-colon
        modified_iso_str = iso_str[:-1] + "+0000"
        return int(1000 * datetime.strptime(modified_iso_str, "%Y-%m-%dT%H:%M:%S%z").timestamp())
        

def get_intervals(minus_x_hours = 48):
    lower_bound = int((datetime.now().timestamp() - minus_x_hours * 60 * 60 ) * 1000)
    page_size = 0
    completed = False
    while page_size < 1000 and completed == False:
        page_size += 50
        intervals = []
        r = requests.get("https://hubspot.clockify.me/api/v1/workspaces/" + workspace + "/user/" + my_id + "/time-entries?page-size=" + str(page_size), headers=headers)
        my_time_entries = r.json()
        for time_entry in my_time_entries:
            assert time_entry["timeInterval"]["start"][-1] == "Z"
            time_start = isostr_to_ms(time_entry["timeInterval"]["start"])
            time_end = isostr_to_ms(time_entry["timeInterval"]["end"])
            if time_end > lower_bound:
                intervals.append([time_start, time_end])
            else:
                completed = True
                break
    return intervals

def tag_activity(activity):
    # TODO implement tag_activity based off a configuration file
    return common_tags["lunch"]

def map_domain(domain):
    try:
        row = domain_dict[domain]
        return row["project_id"], row["tag_id"], row["customer_alias"]
    except KeyError:
        print(domain + " not found in customer_data")
        return None, None, None

def map_domain_csv(domain_csv):
    domains = domain_csv.split(";")
    for domain in domains:
        project, tag, customer_alias = map_domain(domain)
        if project and tag:
            return project, tag, customer_alias
    return None, None, None

def sanitize(description):
    description = re.sub(r"[^a-zA-Z0-9]"," ",description)
    if description == "":
        return ""
    while "  " in description:
        description = description.replace("  "," ")
    if description[-1] == " ":
        description = description[:-1]
    return description.replace("  ", " ")

def log_activity(from_timestamp, to_timestamp, description, project_str, tag_list, billable):
    from_isoZ = datetime.fromtimestamp(from_timestamp/1000, timezone.utc).isoformat()[:-6]+'.000Z'
    to_isoZ = datetime.fromtimestamp(to_timestamp/1000, timezone.utc).isoformat()[:-6]+'.000Z'
    data = {
        "start": from_isoZ,
        "end": to_isoZ,
        "billable": billable,
        "projectId": project_str,
        "tagIds": tag_list,
        "description": description
    }
    r = requests.post("https://hubspot.clockify.me/api/v1/workspaces/" + workspace + "/time-entries", headers=headers, json = data)
    if r:
        return True
    else:
        print("request failure")
        return False

def effective_meeting_times(from_timestamp, to_timestamp):
    from_timestamp = int(from_timestamp)
    to_timestamp = int(to_timestamp)
    skip = False
    original_length = to_timestamp - from_timestamp
    latest_start_date = from_timestamp + original_length * max_meeting_start_delay
    for interval in logged_intervals:
        if latest_start_date > interval[1] > from_timestamp:
            from_timestamp = interval[1]
    for interval in logged_intervals:
        if to_timestamp < interval[0] < to_timestamp:
            to_timestamp = interval[0]
    if (to_timestamp - from_timestamp) < original_length * min_adjusted_meeting_length:
        skip = True
    for interval in logged_intervals:
        if interval[0] < to_timestamp and interval[1] > from_timestamp:
            skip = True
    if skip:
        return None, None
    else:
        return from_timestamp, to_timestamp

def effective_email_times(send_timestamp):
    send_timestamp = int(send_timestamp)
    skip = False
    upper_bound = send_timestamp
    for interval in logged_intervals:
        if interval[1] > send_timestamp and interval[0] < upper_bound:
            upper_bound = interval[0]
    upper_bound = min(upper_bound, send_timestamp)
    for interval in logged_intervals:
        lower_bound = upper_bound - max_email_minutes * 1000 * 60
        if interval[0] < upper_bound and interval[1] > lower_bound:
            lower_bound = interval[1]
    if (upper_bound - lower_bound) * 1000 * 60 < min_email_minutes:
        skip = True
    if (send_timestamp - upper_bound) * 1000 * 60 > max_email_overlap:
        skip = True
    # this loop may be redundant, not sure
    for interval in logged_intervals:
        if interval[0] < upper_bound and interval[1] > lower_bound:
            skip = True
    if skip:
        return None, None
    else:
        return lower_bound, upper_bound


######
# Read customer data from HubDB
######

hubspot = HubSpot()

hubspot.access_token = hs_token

try:
    rows = hubspot.cms.hubdb.rows_api.get_table_rows(table_id_or_name='hsps_clockify_gsuite', sort=['result'], limit=50)
except ApiException as e:
    raise

domain_dict = {}
customer_dict = {}

for row in rows.results:
    row_dict = row.values
    domain_dict[row_dict["domain"]] = row_dict.copy()
    del row_dict["domain"]
    customer_dict[row_dict["customer_alias"]] = row_dict


######
# data loading
######

logged_intervals = get_intervals(36)

engagement_schemata = {
    "customer_meetings": ["start_timestamp", "end_timestamp", "event_summary", "recipient_domains"],
    "email_sent": ["send_timestamp", "subject", "recipient_domains"]
}

engagements = {}

for sheet in engagement_schemata:
    response = requests.get(sheet_base_url + sheet)
    rows = response.content.decode('UTF-8').replace("\"", "").split("\n")[1:]
    engagements[sheet] = []
    for row in rows:
        row = row.split(",")
        row_dict = {}
        for i in range(len(engagement_schemata[sheet])):
            row_dict[engagement_schemata[sheet][i]] = row[i]
        engagements[sheet].append(row_dict)
for meeting in engagements["customer_meetings"]:
    meeting["project"], meeting["tag"], meeting["customer_alias"] = map_domain_csv(meeting["recipient_domains"])
    meeting["event_summary"] = sanitize(meeting["event_summary"]).lower().strip()

for email in engagements["email_sent"]:
    email["project"], email["tag"], email["customer_alias"] = map_domain_csv(email["recipient_domains"])
    email["subject"] = sanitize(email["subject"]).lower().strip()


######
# external interface
######

def tag_activities():
    # TODO tag existing activities based on input files (cf. update_input_files function)
    # TODO find a way to log internal meetings, too
    pass

def log_meetings(silent=False, prep_time_max=0, post_time_max=0):
    # TODO in GAS, exclude meetings everybody but yourself have declined (optional?)
    for row in engagements["customer_meetings"]:
        if row["project"] and row["project"] != "":
            from_timestamp, to_timestamp = effective_meeting_times(row['start_timestamp'], row['end_timestamp'])
            if from_timestamp and to_timestamp and row['project'] and row['tag']:
                r = log_activity(from_timestamp, to_timestamp, "CALL " + row['event_summary'], row['project'], [row['tag'], common_tags["meeting"]], True)
                if r:
                    if not silent:
                        print("Logged call (" + str(round((to_timestamp - from_timestamp)/(1000 * 60)))+ "min) " + "\"" + row['event_summary'] + "\" to " + row['customer_alias'].upper() )
                    logged_intervals.append([from_timestamp, to_timestamp])
                    # prep_call_time
                    prep_from, prep_to = effective_meeting_times(from_timestamp - prep_time_max * 1000 * 60, from_timestamp)
                    if prep_to == from_timestamp and (prep_to - prep_from) / (1000 * 60) > prep_time_max / 2:
                        r = log_activity(prep_from, prep_to, "call_PREP " + row['event_summary'], row['project'], [row['tag'], common_tags["prep_call"]], True)
                        if not r:
                            pass
                            #print("failed to log call_prep for " + row['customer_alias'].upper() )
                    # post_call_time
                    post_from, post_to = effective_meeting_times(to_timestamp, to_timestamp + post_time_max * 1000 * 60)
                    if post_from == to_timestamp  and (post_to - post_from) / (1000 * 60) > post_time_max / 2:
                        r = log_activity(post_from, post_to, "call_POST " + row['event_summary'], row['project'], [row['tag'], common_tags["post_call"]], True)
                        if not r:
                            pass
                            #print("failed to log post_call for " + row['customer_alias'].upper() )
                else:
                    print("FAILED to log call \"" + row['event_summary'] + "\" to " + row['customer_alias'].upper() )
            else:
                print("Cannot log call \"" + row['event_summary'] + "\" to " + row['customer_alias'].upper() + " (coincides with logged activity)")

def log_email(silent=False):
    # TODO truncate subject line when logging activity
    for row in engagements["email_sent"]:
        if row["project"] and row["project"] != "":
            from_timestamp, to_timestamp = effective_email_times(row['send_timestamp'])
            if from_timestamp and to_timestamp and row['project'] and row['tag']:
                r = log_activity(from_timestamp, to_timestamp, "EMAIL " + row['subject'], row['project'], [row['tag'], common_tags["email"]], True)
                if r:
                    if not silent:
                        print("Logged email (" + str(round((to_timestamp - from_timestamp)/(1000 * 60)))+ "min) " + "\"" + row['subject'] + "\" to " + row['customer_alias'].upper() )
                    logged_intervals.append([from_timestamp, to_timestamp])
                else:
                    print("FAILED to log email \"" + row['subject'] + "\" to " + row['customer_alias'].upper() )
            else:
                print("Cannot log email \"" + row['subject'] + "\" to " + row['customer_alias'].upper() + " (coincides with logged activity)")

def fill_general_time(from_iso, until_iso, total_hours_max = 8):
    # TODO white noise function
    # could just book one big block and then call disjointify_activities
    pass

def disjointify_activities(from_iso, until_iso, high_prio_proj = [], low_prio_proj = [common_projects["general_time"]]):
    # TODO write disjointify_activities
    # priority rule is : (not in low > low), and (high > not in high)
    # need to enforce minimum block length
    pass

######
# main
######

def main(event):
    # tag_activities()
    log_meetings(silent=False, prep_time_max=10, post_time_max=5)
    log_email()
    # fill_general_time (whatever params)
    return {
        "outputFields": {
        }
    }

if __name__ == "__main__":
    main(None)