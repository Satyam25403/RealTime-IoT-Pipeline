"""
API key resolution — Layer 1 / Layer 6 (managed identity).

Single function both local dev and Azure deployment call the same way, so
TimerTriggerCityPoll/__init__.py never needs an if/else branch on
environment — the branching lives here, once.

Local dev: reads OWM_API_KEY directly from the environment (populated from
local.settings.json by the Functions runtime, or from .env — see README.md
section 5a).

Azure: reads the secret from Key Vault using DefaultAzureCredential, which
resolves to the Function App's user-assigned managed identity automatically
when running in Azure (see README.md Layer 6 — user-assigned identity
decision) and falls back through other credential types (Azure CLI login,
etc.) when run locally with `az login` but without the emulator env var set.
No connection string or secret is ever read from a config FILE in the Azure
path — only from Key Vault, by identity.
"""

import os
import logging

logger = logging.getLogger("key_vault")

KEY_VAULT_URL_ENV = "KEY_VAULT_URL"
SECRET_NAME = "owm-api-key"


def get_owm_api_key() -> str:
    """
    Returns:
        The OpenWeatherMap API key, from whichever source is appropriate for
        the current environment.

    Raises:
        RuntimeError: if no key could be resolved from either source — this
            should fail the whole poll cycle loudly (not per-city), since
            without a key NO city can be polled.
    """
    local_key = os.environ.get("OWM_API_KEY")
    if local_key:
        logger.debug("resolved OWM API key from OWM_API_KEY env var (local dev path)")
        return local_key

    vault_url = os.environ.get(KEY_VAULT_URL_ENV)
    if not vault_url:
        raise RuntimeError(
            "No OWM API key available: OWM_API_KEY env var is unset AND "
            f"{KEY_VAULT_URL_ENV} env var is unset, so Key Vault can't be "
            "reached either. Set one of these — see README.md section 5a "
            "for local dev or infra/bicep/keyvault.bicep for the Azure path."
        )

    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError as exc:
        raise RuntimeError(
            "azure-identity / azure-keyvault-secrets not installed but "
            f"{KEY_VAULT_URL_ENV} is set — check requirements.txt"
        ) from exc

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    secret = client.get_secret(SECRET_NAME)
    logger.debug("resolved OWM API key from Key Vault (%s)", vault_url)
    return secret.value
