######
# Imports
######

import requests
import pandas as pd
from dotenv import dotenv_values

######
# Configuration
######

env_config = dotenv_values(".env")
api_key = env_config["CLOCKIFYAPI"]

headers = {'x-api-key': api_key}

r = requests.get('https://hubspot.clockify.me/api/v1/user', headers=headers)
context = r.json()
my_id = context["id"]
workspace = context["activeWorkspace"]

######
# data loading
######

customer_project_tag = pd.read_csv("input_files/customer_project_tag.csv",
                                   usecols=["customer_alias", "hub_id", "tag_alias", "project_id"],
                                   dtype={"customer_alias": "string", "hub_id": "int64", "tag_alias": "string", "project_id": "string"},
                                   index_col = "customer_alias")

all_projects = {}
r = requests.get("https://hubspot.clockify.me/api/v1/workspaces/" + workspace + "/projects?page-size=5000", headers=headers)
projects = r.json()
#print(len(projects))
for project in projects:
    if project["archived"] == False:
        all_projects[project["name"]] = project["id"]

def hub_id_to_project_id(hub_id):
    for project in all_projects:
        if str(hub_id) in project:
            return all_projects[project]

customer_project_tag["project_id"] = customer_project_tag.apply(lambda x: hub_id_to_project_id(x["hub_id"]), axis=1)

customer_project_tag.to_csv("input_files/customer_project_tag.csv")