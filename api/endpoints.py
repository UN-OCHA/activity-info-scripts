from typing import Any, List

from pydantic import ValidationError

from api.client import ActivityInfoHTTPClient, APIError
from api.models import (
    DatabaseTree, FormSchema, OperationMetricConfigurationField,
    OperationDataFormsField, Database, AddFormDTO, UpdateDatabaseDTO,
    RecordUpdateDTO, DatabaseTranslations, UpdateDatabaseTranslationsDTO,
    DatabaseUser, AddDatabaseUserDTO, UpdateDatabaseUserRoleDTO, AddDatabaseDTO,
    Resource
)

RawFormPayload = dict[str, Any]


class ActivityInfoEndpoints:
    def __init__(self, http: ActivityInfoHTTPClient):
        self._http = http

    def get_database_tree(self, database_id: str) -> DatabaseTree:
        raw = self._http.request("GET", f"databases/{database_id}")
        try:
            return DatabaseTree.model_validate(raw)
        except ValidationError as e:
            raise APIError("Response does not match DatabaseTree schema") from e

    def add_database(self, dto: AddDatabaseDTO):
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
        return self._http.request("GET", f"form/{form_id}/query")

    def get_form_schema(self, form_id: str) -> FormSchema:
        raw = self._http.request("GET", f"form/{form_id}/schema")
        try:
            return FormSchema.model_validate(raw)  # parse dict into Pydantic model
        except ValidationError as e:
            raise APIError("Response does not match FormSchema") from e

    def get_operation_metric_configuration_fields(
            self,
            form_id: str,
    ) -> List[OperationMetricConfigurationField]:
        raw = self.get_form(form_id)
        try:
            return [
                OperationMetricConfigurationField.model_validate(item)
                if isinstance(item, dict) else item
                for item in raw
            ]
        except ValidationError as e:
            raise APIError("Form does not match OperationMetricConfigurationField schema") from e

    def get_operation_data_forms_fields(
            self,
            form_id: str,
    ) -> List[OperationDataFormsField]:
        raw = self.get_form(form_id)
        try:
            return [
                OperationDataFormsField.model_validate(item)
                if isinstance(item, dict) else item
                for item in raw
            ]
        except ValidationError as e:
            raise APIError("Form does not match OperationDataFormsField schema") from e

    def get_user_databases(self) -> List[Database]:
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
        raw = self._http.request("GET", f"databases/{database_id}/dictionary/database/{language_code}")
        try:
            return DatabaseTranslations.model_validate(raw)
        except ValidationError as e:
            raise APIError("Response does not match DatabaseTranslations schema") from e

    def update_form_records(self, records: List[RecordUpdateDTO]) -> None:
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
        # We manually build the payload to ensure it matches the AddFormRequest structure exactly
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
        raw = self._http.request("GET", f"databases/{database_id}/dictionary/formId/{form_id}/{language_code}")
        try:
            return DatabaseTranslations.model_validate(raw)
        except ValidationError as e:
            raise APIError("Response does not match DatabaseTranslations schema") from e

    def update_form_translations(self, form_id: str, language_code: str, dto: UpdateDatabaseTranslationsDTO):
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
        self._http.request("DELETE", f"databases/{database_id}/users/{user_id}")
