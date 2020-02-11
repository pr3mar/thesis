import requests
import json
import os
from datetime import datetime
from requests.auth import HTTPBasicAuth
from gzip import GzipFile
# assumes that working directory is git repo root (`thesis/`)
# a hack to import from parent directory
import code.config as config
USE_CACHE = True


def load_cache(fname):
    with GzipFile(fname, 'r') as file:
        return json.loads(file.read().decode('utf-8'))


def store_cache(fname, data):
    with GzipFile(fname, 'w') as file:
        return file.write(json.dumps(data).encode('utf-8'))


def get_all_issues():
    fname = f"{data_dir}/issues.gz"
    if USE_CACHE and os.path.isfile(fname):
        print(f"Using cached result from {fname}")
        return load_cache(fname)
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
    store_cache(fname, issues)
    return issues


def get_changelogs(issues):
    fname = f"{data_dir}/changelogs.gz"
    if USE_CACHE and os.path.isfile(fname):
        print(f"Using cached result from {fname}")
        return load_cache(fname)
    changelogs = [get_issue_changelog(issue) for issue in issues]
    store_cache(fname, changelogs)
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
    fname = f"{data_dir}/comments.gz"
    if USE_CACHE and os.path.isfile(fname):
        print(f"Using cached result from {fname}")
        return load_cache(fname)
    comments = [get_issue_comments(issue) for issue in issues]
    store_cache(fname, comments)
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
    fname = f"{data_dir}/users.gz"
    if USE_CACHE and os.path.isfile(fname):
        print(f"Using cached result from {fname}")
        return load_cache(fname)
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
    store_cache(fname, users)
    return users


def exec_get(url, query, append=None):
    resp = requests.request("GET", url, headers=headers, params=query, auth=auth)
    issue_response = json.loads(resp.text)
    print(issue_response)
    if append is not None:
        print(f"Working on {append}: [{issue_response['startAt']:5}/{issue_response['total']:6}]")
    else:
        print(f"Working on [{issue_response['startAt']:5}/{issue_response['total']:6}]")
    return issue_response, (issue_response["startAt"] + issue_response['maxResults']) >= issue_response["total"]


if __name__ == '__main__':
    data_dir = config.data_root
    base_url = "https://celtra.atlassian.net/"
    auth = HTTPBasicAuth(config.jiraUsername, config.jiraPassword)
    headers = {"Accept": "application/json"}

    issues = get_all_issues()
    changelogs = get_changelogs(issues)
    comments = get_comments(issues)
    users = get_users()
    print(len(issues))
    print(len(changelogs))
    print(len(comments))
    print(len(users))
