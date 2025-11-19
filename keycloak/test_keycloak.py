"""
Tests pour le connecteur Keycloak.

Ces tests vérifient que le connecteur fonctionne correctement dans différents cas :
- Authentification réussie
- Gestion des erreurs (401, 500, timeout)
- Secret Vault manquant ou invalide
"""
import pytest
import httpx
from unittest.mock import Mock, patch, MagicMock

from bp2i_airflow_library.connectors.keycloak import KeycloakConnector
from bp2i_airflow_library.schemas.keycloak import KeycloakClient


# ========================================
# FIXTURES : Données réutilisables pour les tests
# ========================================

@pytest.fixture
def mock_product_context():
    """
    Crée un faux contexte produit pour les tests.
    On ne veut pas utiliser le vrai pour ne pas toucher à l'environnement réel.
    """
    context = Mock()
    context.base_product = "test_product"
    return context


@pytest.fixture
def mock_vault():
    """
    Crée un faux Vault qui retourne un secret valide.
    Comme ça on ne va pas chercher dans le vrai Vault pendant les tests.
    """
    vault = Mock()
    # Quand on appelle vault.get_secret(), il retourne ce dictionnaire
    vault.get_secret.return_value = {
        "gateway_url": "https://gateway.example.com",
        "keycloak_url": "https://keycloak.example.com/auth/token",
        "keycloak_data": {
            "client_id": "products_test_product_confidential",
            "client_secret": "super_secret_key",
            "grant_type": "client_credentials",
        },
    }
    return vault


@pytest.fixture
def connector(mock_product_context, mock_vault):
    """
    Crée un connecteur Keycloak pour les tests.
    On réutilise cette fixture dans tous les tests pour éviter de répéter le code.
    """
    return KeycloakConnector(
        product_context=mock_product_context,
        vault=mock_vault,
        timeout=10.0,
    )


@pytest.fixture
def valid_token_response():
    """
    Retourne une réponse Keycloak valide comme si on avait réussi à s'authentifier.
    """
    return {
        "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.fake_token",
        "token_type": "Bearer",
        "expires_in": 300,
    }


# ========================================
# TESTS : Cas d'authentification réussie
# ========================================

@patch('bp2i_airflow_library.connectors.keycloak.httpx.Client')
@patch('bp2i_airflow_library.connectors.keycloak.log_response_error_and_raise')
def test_authentication_success(mock_log, mock_client_class, connector, valid_token_response):
    """
    TEST 1 : Vérifie que l'authentification fonctionne correctement.
    
    Ce test simule :
    1. Un appel réussi à Keycloak
    2. Keycloak retourne un token valide
    3. On vérifie qu'on obtient bien un KeycloakClient avec le bon token
    """
    # On crée une fausse réponse HTTP qui retourne notre token
    mock_response = Mock()
    mock_response.json.return_value = valid_token_response
    
    # On configure httpx.Client pour retourner cette fausse réponse
    mock_client = MagicMock()
    mock_client.__enter__.return_value.post.return_value = mock_response
    mock_client_class.return_value = mock_client
    
    # QUAND : on appelle get_config
    result = connector.get_config()
    
    # ALORS : on vérifie que tout est correct
    assert isinstance(result, KeycloakClient)
    assert result.gateway_url == "https://gateway.example.com"
    assert "Authorization" in result.headers
    assert result.headers["Authorization"].startswith("Bearer ")
    assert "fake_token" in result.headers["Authorization"]


@patch('bp2i_airflow_library.connectors.keycloak.httpx.Client')
@patch('bp2i_airflow_library.connectors.keycloak.log_response_error_and_raise')
def test_custom_secret_name(mock_log, mock_client_class, connector, valid_token_response):
    """
    TEST 2 : Vérifie qu'on peut utiliser un nom de secret personnalisé.
    
    Par exemple : "keycloak_client_prod" au lieu de "keycloak_client"
    """
    mock_response = Mock()
    mock_response.json.return_value = valid_token_response
    
    mock_client = MagicMock()
    mock_client.__enter__.return_value.post.return_value = mock_response
    mock_client_class.return_value = mock_client
    
    # QUAND : on appelle avec un nom de secret personnalisé
    connector.get_config(secret_name="keycloak_custom")
    
    # ALORS : Vault doit avoir été appelé avec ce nom
    call_args = connector.vault.get_secret.call_args
    assert "keycloak_custom" in call_args.kwargs["secret_path"]


# ========================================
# TESTS : Gestion des erreurs HTTP
# ========================================

@patch('bp2i_airflow_library.connectors.keycloak.httpx.Client')
def test_http_error_401(mock_client_class, connector):
    """
    TEST 3 : Vérifie qu'une erreur 401 (Unauthorized) est bien gérée.
    
    Cela arrive quand les credentials sont invalides.
    """
    # On simule une erreur 401
    mock_client = MagicMock()
    mock_client.__enter__.return_value.post.side_effect = httpx.HTTPStatusError(
        "Unauthorized",
        request=Mock(),
        response=Mock(status_code=401)
    )
    mock_client_class.return_value = mock_client
    
    # QUAND : on appelle get_config avec des credentials invalides
    # ALORS : ça doit lever une exception HTTPStatusError
    with pytest.raises(httpx.HTTPStatusError):
        connector.get_config()


@patch('bp2i_airflow_library.connectors.keycloak.httpx.Client')
def test_http_error_500(mock_client_class, connector):
    """
    TEST 4 : Vérifie qu'une erreur 500 (serveur) est bien gérée.
    
    Cela arrive quand le serveur Keycloak a un problème.
    """
    mock_client = MagicMock()
    mock_client.__enter__.return_value.post.side_effect = httpx.HTTPStatusError(
        "Server Error",
        request=Mock(),
        response=Mock(status_code=500)
    )
    mock_client_class.return_value = mock_client
    
    # QUAND/ALORS : ça doit lever une exception
    with pytest.raises(httpx.HTTPStatusError):
        connector.get_config()


@patch('bp2i_airflow_library.connectors.keycloak.httpx.Client')
def test_timeout_error(mock_client_class, connector):
    """
    TEST 5 : Vérifie qu'un timeout est bien géré.
    
    Cela arrive quand Keycloak met trop de temps à répondre.
    """
    mock_client = MagicMock()
    mock_client.__enter__.return_value.post.side_effect = httpx.TimeoutException(
        "Request took too long"
    )
    mock_client_class.return_value = mock_client
    
    # QUAND/ALORS : ça doit lever une TimeoutException
    with pytest.raises(httpx.TimeoutException):
        connector.get_config()


# ========================================
# TESTS : Secrets Vault invalides
# ========================================

def test_vault_secret_missing_gateway_url(connector, mock_vault):
    """
    TEST 6 : Vérifie qu'on détecte un secret Vault incomplet.
    
    Ici : il manque "gateway_url"
    """
    # On change le secret pour qu'il soit incomplet
    mock_vault.get_secret.return_value = {
        "keycloak_url": "https://keycloak.example.com/token",
        "keycloak_data": {
            "client_id": "test",
            "client_secret": "secret",
            "grant_type": "client_credentials",
        },
    }
    
    # QUAND/ALORS : ça doit lever une KeyError
    with pytest.raises(KeyError):
        connector.get_config()


def test_vault_secret_missing_keycloak_data(connector, mock_vault):
    """
    TEST 7 : Vérifie qu'on détecte un secret sans keycloak_data.
    """
    mock_vault.get_secret.return_value = {
        "gateway_url": "https://gateway.example.com",
        "keycloak_url": "https://keycloak.example.com/token",
        # Pas de keycloak_data !
    }
    
    # QUAND/ALORS : ça doit lever une KeyError
    with pytest.raises(KeyError):
        connector.get_config()


# ========================================
# TESTS : Réponses Keycloak invalides
# ========================================

@patch('bp2i_airflow_library.connectors.keycloak.httpx.Client')
@patch('bp2i_airflow_library.connectors.keycloak.log_response_error_and_raise')
def test_invalid_json_response(mock_log, mock_client_class, connector):
    """
    TEST 8 : Vérifie qu'on gère les réponses JSON invalides.
    
    Cela arrive si Keycloak retourne du texte au lieu de JSON.
    """
    mock_response = Mock()
    mock_response.json.side_effect = ValueError("Invalid JSON")
    
    mock_client = MagicMock()
    mock_client.__enter__.return_value.post.return_value = mock_response
    mock_client_class.return_value = mock_client
    
    # QUAND/ALORS : ça doit lever une ValueError
    with pytest.raises(ValueError):
        connector.get_config()


@patch('bp2i_airflow_library.connectors.keycloak.httpx.Client')
@patch('bp2i_airflow_library.connectors.keycloak.log_response_error_and_raise')
def test_missing_access_token_in_response(mock_log, mock_client_class, connector):
    """
    TEST 9 : Vérifie qu'on détecte un token manquant dans la réponse.
    
    Keycloak devrait toujours retourner "access_token", mais si ce n'est pas le cas...
    """
    # Réponse sans access_token
    mock_response = Mock()
    mock_response.json.return_value = {
        "token_type": "Bearer",
        "expires_in": 300,
        # Pas d'access_token !
    }
    
    mock_client = MagicMock()
    mock_client.__enter__.return_value.post.return_value = mock_response
    mock_client_class.return_value = mock_client
    
    # QUAND/ALORS : ça doit lever une KeyError
    with pytest.raises(KeyError):
        connector.get_config()


# ========================================
# COMMENT LANCER LES TESTS
# ========================================
"""
Pour lancer ces tests :

1. Installer les dépendances :
   pip install pytest pytest-mock httpx

2. Lancer tous les tests :
   pytest test_keycloak.py -v

3. Lancer un test spécifique :
   pytest test_keycloak.py::test_authentication_success -v

4. Voir la couverture de code :
   pytest test_keycloak.py --cov=bp2i_airflow_library.connectors.keycloak

Le flag -v (verbose) affiche plus de détails sur chaque test.
"""