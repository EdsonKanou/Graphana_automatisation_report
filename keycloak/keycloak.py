"""
Connecteur Keycloak pour l'authentification via httpx.

Ce module gère l'authentification avec Keycloak pour obtenir un token d'accès.
"""
import logging
import httpx
from bp2i_airflow_library.config import ENVIRONMENT
from bp2i_airflow_library.connectors.base import log_response_error_and_raise
from bp2i_airflow_library.connectors.vault import Vault
from bp2i_airflow_library.dependencies.product import ProductContext
from bp2i_airflow_library.schemas.keycloak import KeycloakClient

logger: logging.Logger = logging.getLogger(__name__)


class KeycloakConnector:
    """
    Connecteur pour l'authentification Keycloak.
    
    Ce connecteur récupère les credentials depuis Vault et obtient un token
    d'accès auprès de Keycloak pour authentifier les requêtes API.
    """
    
    def __init__(
        self,
        product_context: ProductContext,
        vault: Vault,
        timeout: float = 30.0,
    ) -> None:
        """
        Initialise le connecteur Keycloak.
        
        Args:
            product_context: Le contexte du produit (contient le nom du produit)
            vault: L'instance Vault pour récupérer les secrets
            timeout: Temps maximum d'attente pour les requêtes (en secondes)
        """
        self.product_context = product_context
        self.vault = vault
        self.timeout = timeout

    def get_config(self, secret_name: str | None = None) -> KeycloakClient:
        """
        Récupère la configuration et authentifie avec Keycloak.
        
        Cette méthode :
        1. Récupère le secret depuis Vault
        2. Envoie une requête POST à Keycloak pour obtenir un token
        3. Retourne un client configuré avec le token
        
        Args:
            secret_name: Nom du secret dans Vault (par défaut: "keycloak_client")
            
        Returns:
            KeycloakClient: Client configuré avec l'URL gateway et les headers
            
        Raises:
            httpx.HTTPStatusError: Si Keycloak retourne une erreur (401, 500, etc.)
            httpx.TimeoutException: Si la requête prend trop de temps
            KeyError: Si le secret Vault est incomplet
        """
        # Si pas de nom fourni, on utilise le nom par défaut
        secret_name = secret_name or "keycloak_client"
        
        # Étape 1 : Récupérer le secret depuis Vault
        secret_path = f"orchestrator/{ENVIRONMENT}/products/{self.product_context.base_product}/{secret_name}"
        secret = self.vault.get_secret(secret_path=secret_path)
        
        # Étape 2 : Authentification avec Keycloak via httpx
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(
                url=secret["keycloak_url"],
                data=secret["keycloak_data"],
            )
            
            # Vérifie si la requête a réussi (si erreur, cette fonction raise)
            log_response_error_and_raise(response)
            
            # Récupère les données JSON de la réponse
            keycloak_data = response.json()
        
        # Étape 3 : Construire et retourner le client Keycloak
        return KeycloakClient(
            gateway_url=secret["gateway_url"],
            headers={
                "Authorization": f"{keycloak_data['token_type']} {keycloak_data['access_token']}",
                "accept": "application/json",
            },
        )