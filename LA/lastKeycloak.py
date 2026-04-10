import logging
import os

import httpx

from bp2i_airflow_library.config import ENVIRONMENT
from bp2i_airflow_library.connectors.base import log_response_error_and_raise
from bp2i_airflow_library.connectors.vault import Vault
from bp2i_airflow_library.dependencies.product import ProductContext
from bp2i_airflow_library.schemas.keycloak import KeycloakClient

logger: logging.Logger = logging.getLogger(__name__)


class KeycloakConnector:
    def __init__(
        self, product_context: ProductContext, vault: Vault
    ) -> None:
        self.product_context = product_context
        self.vault = vault

    def get_config(
        self, secret_name: str | None = None
    ) -> KeycloakClient:
        secret_name = secret_name or "keycloak_client"
        secret = self.vault.get_secret(
            secret_path=f"orchestrator/{ENVIRONMENT}/products/{self.product_context.base_product}/{secret_name}",
        )

        # Récupération du certificat CA BNPP (équivalent REQUESTS_CA_BUNDLE)
        ca_bundle = os.environ.get("REQUESTS_CA_BUNDLE", "/etc/cacerts/bppca")

        with httpx.Client(verify=ca_bundle) as client:
            response = client.post(
                url=secret["keycloak_url"],
                data=secret["keycloak_data"],
            )

        log_response_error_and_raise(response)
        keycloak_data = response.json()

        return KeycloakClient(
            gateway_url=secret["gateway_url"],
            headers={
                "Authorization": f"{keycloak_data['token_type']} {keycloak_data['access_token']}",
                "accept": "application/json",
            },
        )