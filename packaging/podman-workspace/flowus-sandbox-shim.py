#!/usr/bin/env python3
"""Sandbox-side FlowUs CLI shim backed by the restricted host Unix bridge."""

import http.client
import json
import os
import sys

SOCKET_PATH = os.environ.get("FLOWUS_SOCKET", "")
MAX_RESPONSE = 16_777_216


def main() -> int:
    if not SOCKET_PATH:
        print("flowus: FLOWUS_SOCKET is not configured", file=sys.stderr)
        return 78
    request_body = json.dumps({"args": sys.argv[1:], "cwd": os.getcwd()}).encode()
    connection = http.client.HTTPConnection("localhost", timeout=920)
    connection.sock = None
    try:
        connection.connect = lambda: _connect_unix(connection)  # type: ignore[method-assign]
        connection.request(
            "POST",
            "/flowus/run",
            body=request_body,
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        payload = response.read(MAX_RESPONSE + 1)
        if len(payload) > MAX_RESPONSE:
            print("flowus: bridge response too large", file=sys.stderr)
            return 70
        if response.status >= 400:
            text = payload.decode("utf-8", errors="replace").strip()
            print(f"flowus: bridge HTTP {response.status}: {text}", file=sys.stderr)
            return 77
        try:
            result = json.loads(payload)
            code = int(result["code"])
            stdout = result.get("stdout", "")
            stderr = result.get("stderr", "")
            if not isinstance(stdout, str) or not isinstance(stderr, str):
                raise TypeError("stdout/stderr must be strings")
        except (ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
            print(f"flowus: invalid bridge response: {error}", file=sys.stderr)
            return 70
        if stdout:
            sys.stdout.write(stdout)
        if stderr:
            sys.stderr.write(stderr)
        return code
    except (OSError, http.client.HTTPException) as error:
        print(f"flowus: bridge unavailable: {error}", file=sys.stderr)
        return 69
    finally:
        connection.close()


def _connect_unix(connection: http.client.HTTPConnection) -> None:
    import socket

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(connection.timeout)
    sock.connect(SOCKET_PATH)
    connection.sock = sock


if __name__ == "__main__":
    raise SystemExit(main())
