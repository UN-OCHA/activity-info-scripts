from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.database_lock import DatabaseLock
    from ..models.database_resource import DatabaseResource
    from ..models.database_role import DatabaseRole
    from ..models.grant import Grant
    from ..models.security_category import SecurityCategory
    from ..models.user_ref import UserRef
    from ..models.user_role import UserRole


T = TypeVar("T", bound="Database")


@_attrs_define
class Database:
    """
    Attributes:
        database_id (str): This database's id
        user_id (str): The id of the requesting user. The contents of the tree depends on the permissions of the
            requesting user.
        version (str): The monotonically-increasing version of the database tree. The version number is incremented
            whenever a change is made that affects the database tree.
        label (str): This database's human-readable label.
        owner_ref (None | UserRef):
        suspended (bool): True if this database is suspended for billing reasons
        role (UserRole):
        roles (list[DatabaseRole]): The roles that have been defined for this database.
        storage (str):
        published_template (bool): True if this database has been published as a template.
        security_categories (list[SecurityCategory]): The security categories that have been defined for this database.
        resources (list[DatabaseResource]): The set of resources (folders, forms, and subforms) that belong to this
            database.
        billing_account_id (int):
        billing_plan (str): The billing plan name under which this database falls. The billing plan can have an affect
            on which features are avialable within this database.
        description (str | Unset):
        language (str | Unset): Current database language
        original_language (str | Unset): Original database language
        languages (list[str] | Unset): The list of languages that have been defined for this database, including the
            original language, if set, and any translations.
        locks (list[DatabaseLock] | Unset): The record locks that have been defined on this database.
        grants (list[Grant] | Unset): The direct (non-role) permission grants that have been made to the requesting user
            for this database.
    """

    database_id: str
    user_id: str
    version: str
    label: str
    owner_ref: None | UserRef
    suspended: bool
    role: UserRole
    roles: list[DatabaseRole]
    storage: str
    published_template: bool
    security_categories: list[SecurityCategory]
    resources: list[DatabaseResource]
    billing_account_id: int
    billing_plan: str
    description: str | Unset = UNSET
    language: str | Unset = UNSET
    original_language: str | Unset = UNSET
    languages: list[str] | Unset = UNSET
    locks: list[DatabaseLock] | Unset = UNSET
    grants: list[Grant] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.user_ref import UserRef

        database_id = self.database_id

        user_id = self.user_id

        version = self.version

        label = self.label

        owner_ref: dict[str, Any] | None
        if isinstance(self.owner_ref, UserRef):
            owner_ref = self.owner_ref.to_dict()
        else:
            owner_ref = self.owner_ref

        suspended = self.suspended

        role = self.role.to_dict()

        roles = []
        for roles_item_data in self.roles:
            roles_item = roles_item_data.to_dict()
            roles.append(roles_item)

        storage = self.storage

        published_template = self.published_template

        security_categories = []
        for security_categories_item_data in self.security_categories:
            security_categories_item = security_categories_item_data.to_dict()
            security_categories.append(security_categories_item)

        resources = []
        for resources_item_data in self.resources:
            resources_item = resources_item_data.to_dict()
            resources.append(resources_item)

        billing_account_id = self.billing_account_id

        billing_plan = self.billing_plan

        description = self.description

        language = self.language

        original_language = self.original_language

        languages: list[str] | Unset = UNSET
        if not isinstance(self.languages, Unset):
            languages = self.languages

        locks: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.locks, Unset):
            locks = []
            for locks_item_data in self.locks:
                locks_item = locks_item_data.to_dict()
                locks.append(locks_item)

        grants: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.grants, Unset):
            grants = []
            for grants_item_data in self.grants:
                grants_item = grants_item_data.to_dict()
                grants.append(grants_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "databaseId": database_id,
                "userId": user_id,
                "version": version,
                "label": label,
                "ownerRef": owner_ref,
                "suspended": suspended,
                "role": role,
                "roles": roles,
                "storage": storage,
                "publishedTemplate": published_template,
                "securityCategories": security_categories,
                "resources": resources,
                "billingAccountId": billing_account_id,
                "billingPlan": billing_plan,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if language is not UNSET:
            field_dict["language"] = language
        if original_language is not UNSET:
            field_dict["originalLanguage"] = original_language
        if languages is not UNSET:
            field_dict["languages"] = languages
        if locks is not UNSET:
            field_dict["locks"] = locks
        if grants is not UNSET:
            field_dict["grants"] = grants

        return field_dict

    @classmethod
    def from_dict(cls, src_dict: Mapping[str, Any]) -> Self:
        from ..models.database_lock import DatabaseLock
        from ..models.database_resource import DatabaseResource
        from ..models.database_role import DatabaseRole
        from ..models.grant import Grant
        from ..models.security_category import SecurityCategory
        from ..models.user_ref import UserRef
        from ..models.user_role import UserRole

        d = dict(src_dict)
        database_id = d.pop("databaseId")

        user_id = d.pop("userId")

        version = d.pop("version")

        label = d.pop("label")

        def _parse_owner_ref(data: object) -> None | UserRef:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                owner_ref_type_0 = UserRef.from_dict(data)

                return owner_ref_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | UserRef, data)

        owner_ref = _parse_owner_ref(d.pop("ownerRef"))

        suspended = d.pop("suspended")

        role = UserRole.from_dict(d.pop("role"))

        roles = []
        _roles = d.pop("roles")
        for roles_item_data in _roles:
            roles_item = DatabaseRole.from_dict(roles_item_data)

            roles.append(roles_item)

        storage = d.pop("storage")

        published_template = d.pop("publishedTemplate")

        security_categories = []
        _security_categories = d.pop("securityCategories")
        for security_categories_item_data in _security_categories:
            security_categories_item = SecurityCategory.from_dict(
                security_categories_item_data
            )

            security_categories.append(security_categories_item)

        resources = []
        _resources = d.pop("resources")
        for resources_item_data in _resources:
            resources_item = DatabaseResource.from_dict(resources_item_data)

            resources.append(resources_item)

        billing_account_id = d.pop("billingAccountId")

        billing_plan = d.pop("billingPlan")

        description = d.pop("description", UNSET)

        language = d.pop("language", UNSET)

        original_language = d.pop("originalLanguage", UNSET)

        languages = cast(list[str], d.pop("languages", UNSET))

        _locks = d.pop("locks", UNSET)
        locks: list[DatabaseLock] | Unset = UNSET
        if _locks is not UNSET:
            locks = []
            for locks_item_data in _locks:
                locks_item = DatabaseLock.from_dict(locks_item_data)

                locks.append(locks_item)

        _grants = d.pop("grants", UNSET)
        grants: list[Grant] | Unset = UNSET
        if _grants is not UNSET:
            grants = []
            for grants_item_data in _grants:
                grants_item = Grant.from_dict(grants_item_data)

                grants.append(grants_item)

        database = cls(
            database_id=database_id,
            user_id=user_id,
            version=version,
            label=label,
            owner_ref=owner_ref,
            suspended=suspended,
            role=role,
            roles=roles,
            storage=storage,
            published_template=published_template,
            security_categories=security_categories,
            resources=resources,
            billing_account_id=billing_account_id,
            billing_plan=billing_plan,
            description=description,
            language=language,
            original_language=original_language,
            languages=languages,
            locks=locks,
            grants=grants,
        )

        database.additional_properties = d
        return database

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
