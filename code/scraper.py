import requests
import json
import os
from datetime import datetime
from requests.auth import HTTPBasicAuth
from pathlib import Path

USE_CACHE = True


def get_all_issues():
    fname = f"{data_dir}/issues.json"
    if USE_CACHE and os.path.isfile(fname):
        print(f"Using cached result from {fname}")
        with open(fname) as file:
            return json.load(file)
    url = f"{base_url}/rest/api/3/search"
    start_at = 0
    max_results = 100
    query = {
        "jql": 'project=MAB',
        "maxResults": max_results,
        "expand": ["renderedFields", "editmeta"],
        "startAt": start_at
    }

    obtained_all = False
    issues = []
    while not obtained_all:
        query["startAt"] = start_at
        resp, obtained_all = exec_get(url, query)
        start_at += resp["maxResults"]
        for issue in resp["issues"]:
            issue["api_dateAccessed"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            issues.append(issue)

    with open(fname, "w") as file:
        json.dump(issues, file)
    return issues


def get_changelogs(issues):
    fname = f"{data_dir}/changelogs.json"
    if USE_CACHE and os.path.isfile(fname):
        print(f"Using cached result from {fname}")
        with open(fname) as file:
            return json.load(file)
    changelogs = [get_issue_changelog(issue) for issue in issues]
    with open(fname, "w") as file:
        json.dump(changelogs, file)
    return changelogs


def get_issue_changelog(issue):
    url = f"{base_url}/rest/api/3/issue/{issue['id']}/changelog"
    obtained_all = False
    changelog = []
    max_results = 100
    start_at = 0
    query = {
        "maxResults": max_results,
        "startAt": start_at
    }
    while not obtained_all:
        query["startAt"] = start_at
        resp, obtained_all = exec_get(url, query, append=f'{issue["key"]} changelog')
        start_at += resp["maxResults"]
        changelog = [*changelog, *resp["values"]]
    return {
        "id": issue['id'],
        "key": issue['key'],
        "api_dateAccessed": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "changelog": changelog
    }


def get_comments(issues):
    fname = f"{data_dir}/comments.json"
    if USE_CACHE and os.path.isfile(fname):
        print(f"Using cached result from {fname}")
        with open(fname) as file:
            return json.load(file)
    comments = [get_issue_comments(issue) for issue in issues]
    with open(fname, "w") as file:
        json.dump(comments, file)
    return comments


def get_issue_comments(issue):
    url = f"{base_url}/rest/api/3/issue/{issue['id']}/comment"
    obtained_all = False
    changelog = []
    max_results = 50
    start_at = 0
    query = {
        "maxResults": max_results,
        "startAt": start_at
    }
    while not obtained_all:
        query["startAt"] = start_at
        resp, obtained_all = exec_get(url, query, append=f'{issue["key"]} comments')
        start_at += resp["maxResults"]
        changelog = [*changelog, *resp["comments"]]
    return {
        "id": issue['id'],
        "key": issue['key'],
        "api_dateAccessed": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "changelog": changelog
    }


def get_users():
    fname = f"{data_dir}/users.json"
    if USE_CACHE and os.path.isfile(fname):
        print(f"Using cached result from {fname}")
        with open(fname) as file:
            return json.load(file)
    url = f"{base_url}/rest/api/3/users/search"
    max_results = 1000
    start_at = 0
    query = {"maxResults": max_results, "startAt": start_at}
    users_temp = json.loads(requests.request("GET", url, headers=headers, params=query, auth=auth).text)
    users = []
    for i, user in enumerate(users_temp):
        query = {"accountId": user["accountId"]}
        print(f"[{i + 1}/{len(users_temp)}] Getting user: {user['accountId']}")
        resp = requests.request("GET", f"{base_url}/rest/api/3/user", headers=headers, params=query, auth=auth)
        usr = json.loads(resp.text)
        users.append(usr)
    with open(fname, "w") as file:
        json.dump(users, file)
    return users


def exec_get(url, query, append=None):
    resp = requests.request("GET", url, headers=headers, params=query, auth=auth)
    issue_response = json.loads(resp.text)
    if append is not None:
        print(f"Working on {append}: [{issue_response['startAt']:5}/{issue_response['total']:6}]")
    else:
        print(f"Working on [{issue_response['startAt']:5}/{issue_response['total']:6}]")
    return issue_response, (issue_response["startAt"] + issue_response['maxResults']) >= issue_response["total"]


if __name__ == '__main__':
    data_dir = '../data'
    base_url = "https://celtra.atlassian.net/"
    credentials = open(f"{str(Path.home())}/.atlassian").read().strip().split(":")
    auth = HTTPBasicAuth(credentials[0], credentials[1])
    headers = {"Accept": "application/json"}

    issues = get_all_issues()
    changelogs = get_changelogs(issues)
    comments = get_comments(issues)
    users = get_users()
    print(len(issues))
    print(len(changelogs))
    print(len(comments))
    print(len(users))
