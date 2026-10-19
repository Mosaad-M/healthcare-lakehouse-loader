"""The orchestration API requires authentication: anyone on the network could otherwise
trigger or change flows."""
import os
import urllib.error
import urllib.request

import pytest


def test_the_prefect_api_refuses_unauthenticated_requests():
    url = os.environ["PREFECT_API_URL"] + "/deployments/filter"
    req = urllib.request.Request(url, data=b"{}", method="POST", headers={"Content-Type": "application/json"})
    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(req, timeout=10)
    assert err.value.code == 401
