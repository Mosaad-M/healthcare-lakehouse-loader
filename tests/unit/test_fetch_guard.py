"""Partner API fetches cannot be pointed at internal or metadata addresses, and never follow
redirects (which would carry the bearer token to an unchecked address)."""
import http.server
import socket
import threading
import urllib.error
import urllib.request

import pytest

from loader import fetch


def resolving_to(monkeypatch, address):
    family = socket.AF_INET6 if ":" in address else socket.AF_INET
    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: [(family, socket.SOCK_STREAM, 6, "", (address, 0))])


@pytest.mark.parametrize("address", ["169.254.170.2", "169.254.169.254", "127.0.0.1", "::1", "fe80::1", "0.0.0.0", "224.0.0.1"])
def test_metadata_loopback_and_link_local_are_always_refused(monkeypatch, address):
    monkeypatch.setenv("LOADER_FETCH_ALLOW_PRIVATE", "1")
    resolving_to(monkeypatch, address)
    with pytest.raises(fetch.FetchFailed, match="not allowed"):
        fetch.check_destination("http://partner.example/v1/x")


@pytest.mark.parametrize("address", ["10.0.0.5", "172.20.90.10", "192.168.1.2"])
def test_private_networks_need_the_platform_switch(monkeypatch, address):
    resolving_to(monkeypatch, address)
    monkeypatch.delenv("LOADER_FETCH_ALLOW_PRIVATE", raising=False)
    with pytest.raises(fetch.FetchFailed, match="private"):
        fetch.check_destination("http://partner.example/v1/x")
    monkeypatch.setenv("LOADER_FETCH_ALLOW_PRIVATE", "1")
    fetch.check_destination("http://partner.example/v1/x")


def test_public_addresses_pass(monkeypatch):
    monkeypatch.delenv("LOADER_FETCH_ALLOW_PRIVATE", raising=False)
    resolving_to(monkeypatch, "93.184.216.34")
    fetch.check_destination("https://partner.example/v1/x")


def test_redirects_are_not_followed():
    class Redirect(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(302)
            self.send_header("Location", "http://169.254.169.254/latest/meta-data/")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Redirect)
    threading.Thread(target=server.handle_request, daemon=True).start()
    request = urllib.request.Request(f"http://127.0.0.1:{server.server_port}/", headers={"Authorization": "Bearer t"})
    with pytest.raises(urllib.error.HTTPError) as err:
        fetch._OPENER.open(request, timeout=5)
    assert err.value.code == 302
    server.server_close()
