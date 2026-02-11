"""msgvault_sdk - Python SDK for msgvault email archives."""

from msgvault_sdk.errors import (
    ChangeLogError,
    MsgvaultError,
    QueryError,
    VaultNotFoundError,
    VaultReadOnlyError,
)
from msgvault_sdk.models import (
    Account,
    Attachment,
    Conversation,
    Label,
    Message,
    Participant,
)
from msgvault_sdk.vault import Vault

__all__ = [
    "Account",
    "Attachment",
    "ChangeLogError",
    "Conversation",
    "Label",
    "Message",
    "MsgvaultError",
    "Participant",
    "QueryError",
    "Vault",
    "VaultNotFoundError",
    "VaultReadOnlyError",
]
