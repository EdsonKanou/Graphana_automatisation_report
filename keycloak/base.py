import json
import logging
from typing import Union

import httpx
import requests

from bp2i_airflow_library.utils.log_filter import (
    exclude_lines_from_obfuscation_check,
)

logger: logging.Logger = logging.getLogger(__name__)


def log_response_error_and_raise(
    response: Union[requests.Response, httpx.Response]
) -> None:
    """
    Version compatible requests ET httpx.
    Détecte automatiquement le type de response.
    """
    # Détection du type de response
    is_httpx = isinstance(response, httpx.Response)

    # Vérification du statut (compatible requests et httpx)
    is_success = response.is_success if is_httpx else response.ok

    if not is_success:
        trace_id: str | None = None
        if "x-trace-id" in response.headers:
            trace_id = response.headers["x-trace-id"]

        try:
            response_content = json.dumps(response.json(), indent=4)
        except Exception:
            try:
                response_content = response.content.decode()
            except Exception:
                response_content = response.content

        # Récupération du reason (compatible requests et httpx)
        reason = (
            response.reason_phrase if is_httpx else response.reason
        )

        logger.warning(
            f"[http error]\n"
            f"[=] http request → ({response.request.method}) '{response.request.url}'\n"
            f"[=] status code = ({response.status_code}: '{reason}')\n"
            f"[=] response content =>\n"
            f"{response_content}\n"
            f"{exclude_lines_from_obfuscation_check(f'[=] trace id = {trace_id}{chr(10)}') if trace_id else ''}"
        )

        response.raise_for_status()