from typing import Any, List

from pydantic import ValidationError

from api.client import ActivityInfoHTTPClient, APIError
from api.models import (
    DatabaseTree, FormSchema, Database, AddFormDTO, UpdateDatabaseDTO,
    RecordUpdateDTO, DatabaseTranslations, UpdateDatabaseTranslationsDTO,
    DatabaseUser, AddDatabaseUserDTO, UpdateDatabaseUserRoleDTO, AddDatabaseDTO
)

# Type alias for raw dictionary payloads returned by some API endpoints
RawFormPayload = dict[str, Any]


class ActivityInfoEndpoints:
    """
    Collection of methods representing specific ActivityInfo API endpoints.
    
    This class handles the conversion between high-level Python objects (DTOs) 
    and the raw JSON payloads expected by the ActivityInfo REST API. It also 
    performs validation on incoming responses using Pydantic models.
    """

    def __init__(self, http: ActivityInfoHTTPClient):
        """
        Initialize the endpoints collection.
        
        Args:
            http (ActivityInfoHTTPClient): The authenticated HTTP client to use for requests.
        """
        self._http = http

    def get_database_tree(self, database_id: str) -> DatabaseTree:
        """Fetch the full hierarchical tree structure of a database."""
        raw = self._http.request("GET", f"databases/{database_id}")
        try:
            return DatabaseTree.model_validate(raw)
        except ValidationError as e:
            raise APIError("Response does not match DatabaseTree schema") from e

    def add_database(self, dto: AddDatabaseDTO):
        """Create a new database."""
        self._http.request(
            "POST",
            "databases",
            json=dto.model_dump(
                mode="json",
                exclude_none=True,
                exclude_unset=True,
                by_alias=True,
            )
        )

    def get_form(self, form_id: str) -> List[RawFormPayload]:
        """Query and retrieve all records for a specific form."""
        return self._http.request("GET", f"form/{form_id}/query")

    def get_form_schema(self, form_id: str) -> FormSchema:
        """Retrieve the design/schema (fields, types, formulas) for a specific form."""
        raw = self._http.request("GET", f"form/{form_id}/schema")
        try:
            return FormSchema.model_validate(raw)
        except ValidationError as e:
            raise APIError("Response does not match FormSchema") from e

    def get_user_databases(self) -> List[Database]:
        """List all databases the authenticated user has access to."""
        raw = self._http.request("GET", "databases")
        try:
            return [
                Database.model_validate(item)
                if isinstance(item, dict) else item
                for item in raw
            ]
        except ValidationError as e:
            raise APIError("Item does not match Database schema") from e

    def get_database_translations(self, database_id: str, language_code: str) -> DatabaseTranslations:
        """Fetch all database-level translations for a specific language."""
        raw = self._http.request("GET", f"databases/{database_id}/dictionary/database/{language_code}")
        try:
            return DatabaseTranslations.model_validate(raw)
        except ValidationError as e:
            raise APIError("Response does not match DatabaseTranslations schema") from e

    def update_form_records(self, records: List[RecordUpdateDTO]) -> None:
        """Bulk create, update, or delete records across one or more forms."""
        payload = {
            "changes": [
                r.model_dump(
                    mode="json",
                    exclude_unset=True,
                    by_alias=True,
                )
                for r in records
            ]
        }
        self._http.request("POST", "update", json=payload)

    def update_form_schema(self, schema: FormSchema):
        """Push a modified schema back to the server to update a form's design."""
        self._http.request(
            "POST",
            f"form/{schema.id}/schema",
            json=schema.model_dump(
                mode="json",
                exclude_none=True,
                exclude_unset=True,
                by_alias=True,
            )
        )

    def add_form(self, dto: AddFormDTO):
        """Add a new form to a specific database."""
        # Manually construct the nested payload to strictly match AI's expected structure
        payload = {
            "formResource": {
                "id": dto.formResource.id,
                "parentId": dto.formResource.parentId,
                "label": dto.formResource.label,
                "type": dto.formResource.type,
                "visibility": dto.formResource.visibility or "PRIVATE"
            },
            "formClass": dto.formClass.model_dump(
                mode="json",
                exclude_none=True,
                exclude_unset=True,
                by_alias=True
            )
        }

        self._http.request(
            "POST",
            f"databases/{dto.formClass.databaseId}/forms",
            json=payload
        )

    def update_database(self, database_id: str, dto: UpdateDatabaseDTO):
        """Modify database-level settings (e.g., enabled languages)."""
        self._http.request(
            "POST",
            f"databases/{database_id}",
            json=dto.model_dump(
                mode="json",
                exclude_none=True,
                exclude_unset=True,
                by_alias=True,
            )
        )

    def update_database_translations(self, database_id: str, language_code: str, dto: UpdateDatabaseTranslationsDTO):
        """Update global translations for a specific database."""
        self._http.request(
            "POST",
            f"databases/{database_id}/translations/{language_code}",
            json=dto.model_dump(
                mode="json",
                exclude_none=True,
                exclude_unset=True,
                by_alias=True,
            )
        )

    def get_form_translations(self, database_id: str, form_id: str, language_code: str):
        """Retrieve all translated labels for a specific form and its fields."""
        raw = self._http.request("GET", f"databases/{database_id}/dictionary/formId/{form_id}/{language_code}")
        try:
            return DatabaseTranslations.model_validate(raw)
        except ValidationError as e:
            raise APIError("Response does not match DatabaseTranslations schema") from e

    def update_form_translations(self, form_id: str, language_code: str, dto: UpdateDatabaseTranslationsDTO):
        """Apply new translations to a specific form and its fields."""
        self._http.request(
            "POST",
            f"form/{form_id}/schema/translations/{language_code}",
            json=dto.model_dump(
                mode="json",
                exclude_none=True,
                exclude_unset=True,
                by_alias=True,
            )
        )

    def get_database_users(self, database_id: str):
        """List all users who have access to the specified database."""
        raw = self._http.request("GET", f"databases/{database_id}/users")
        try:
            return [
                DatabaseUser.model_validate(item)
                if isinstance(item, dict) else item
                for item in raw
            ]
        except ValidationError as e:
            raise APIError(
                "Item does not match DatabaseUser schema"
            ) from e

    def add_database_user(self, database_id: str, dto: AddDatabaseUserDTO):
        """Invite or add a new user to a database with a specific role."""
        self._http.request(
            "POST",
            f"databases/{database_id}/users",
            json=dto.model_dump(
                mode="json",
                exclude_none=True,
                exclude_unset=True,
                by_alias=True,
            )
        )

    def update_database_user_role(self, database_id: str, user_id: str, dto: UpdateDatabaseUserRoleDTO):
        """Modify the assigned role for an existing database user."""
        self._http.request(
            "POST",
            f"databases/{database_id}/users/{user_id}/role",
            json=dto.model_dump(
                mode="json",
                exclude_none=True,
                exclude_unset=True,
                by_alias=True,
            )
        )

    def delete_database_user(self, database_id: str, user_id: str):
        """Remove a user's access to a specific database."""
        self._http.request("DELETE", f"databases/{database_id}/users/{user_id}")
