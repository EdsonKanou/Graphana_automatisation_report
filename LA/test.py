#For keycloak migration
#Test 1

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
import requests

from utils import log_response_error_and_raise


def make_requests_response(status=400, trace_id=None):
    resp = requests.Response()
    resp.status_code = status
    resp._content = json.dumps({"error": "invalid"}).encode()
    resp.reason = "Bad Request"

    # Mock request object
    req = requests.Request("GET", "https://example.com").prepare()
    resp.request = req

    # Headers
    resp.headers = {}
    if trace_id:
        resp.headers["x-trace-id"] = trace_id

    return resp


def test_log_error_requests():
    response = make_requests_response(trace_id="abc-123")

    with patch("utils.logger") as mock_logger:
        with pytest.raises(requests.HTTPError):
            log_response_error_and_raise(response)

        # vérifie qu'un warning a été loggé
        mock_logger.warning.assert_called_once()

        logged_msg = mock_logger.warning.call_args[0][0]

        assert "[http error]" in logged_msg
        assert "status code" in logged_msg
        assert "trace id" in logged_msg
        assert "abc-123" in logged_msg


# Test 2


import json
import logging
from unittest.mock import MagicMock, patch

import pytest
import httpx

from utils import log_response_error_and_raise


def make_httpx_response(status=400, trace_id=None):
    req = httpx.Request("GET", "https://example.com")
    resp = httpx.Response(
        status_code=status,
        json={"error": "invalid"},
        request=req,
        headers={"x-trace-id": trace_id} if trace_id else {},
    )
    return resp


def test_log_error_httpx():
    response = make_httpx_response(trace_id="abc-123")

    with patch("utils.logger") as mock_logger:
        with pytest.raises(httpx.HTTPStatusError):
            log_response_error_and_raise(response)

        mock_logger.warning.assert_called_once()

        logged_msg = mock_logger.warning.call_args[0][0]

        assert "[http error]" in logged_msg
        assert "status code" in logged_msg
        assert "trace id" in logged_msg
        assert "abc-123" in logged_msg
