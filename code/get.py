# This code sample uses the 'requests' library:
# http://docs.python-requests.org
import requests
import json
from requests.auth import HTTPBasicAuth
from pathlib import Path


base_url = "https://celtra.atlassian.net/"

# url = f"{base_url}rest/api/3/field"
# url = f"{base_url}rest/api/3/label"
url = f"{base_url}/rest/api/3/resolution"
# url = f"{base_url}rest/api/3/project/{projectIdOrKey}/components"  # set `projectIdOrKey`
# url = f"{base_url}rest/api/3/role/{roleId}/actors"  # need elevated permissionss

credentials = open(f"{str(Path.home())}/.atlassian").read().strip().split(":")

auth = HTTPBasicAuth(credentials[0], credentials[1])

headers = {
   "Accept": "application/json"
}

response = requests.request(
   "GET",
   url,
   headers=headers,
   auth=auth
)

print(json.dumps(json.loads(response.text), sort_keys=True, indent=4, separators=(",", ": ")))
