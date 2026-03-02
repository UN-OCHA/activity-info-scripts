from enum import StrEnum, auto
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field, ConfigDict, field_validator


class DatabaseTreeResourceType(StrEnum):
    FORM = "FORM"
    FOLDER = "FOLDER"
    OTHER = auto()


class DatabaseTreeResourceVisibility(StrEnum):
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"
    REFERENCE = "REFERENCE"


class FieldType(StrEnum):
    serial = "serial"
    month = "month"
    attachment = "attachment"
    geopoint = "geopoint"
    FREE_TEXT = "FREE_TEXT"
    quantity = "quantity"
    enumerated = "enumerated"
    multiselectreference = "reference"
    epiweek = "epiweek"
    subform = "subform"
    date = "date"
    calculated = "calculated"
    reversereference = "reversereference"
    reference = "reference"
    fortnight = "fortnight"
    section = "section"
    NARRATIVE = "NARRATIVE"


class FieldTypeParametersCardinality(StrEnum):
    SINGLE = "SINGLE"
    MULTIPLE = "MULTIPLE"


class OwnerRef(BaseModel):
    id: str
    name: str
    email: str


class FilteredPermission(BaseModel):
    operation: str = Field(alias="operation")
    filter: Optional[str] = Field(default=None, alias="filter")
    securityCategories: List[str] = Field(default_factory=list, alias="securityCategories")


class Grant(BaseModel):
    resourceId: str = Field(alias="resourceId")
    optional: bool = Field(default=False, alias="optional")
    operations: List[FilteredPermission] = Field(default_factory=list, alias="operations")
    conditions: List[Any] = Field(default_factory=list, alias="conditions")


class Role(BaseModel):
    id: str = Field(alias="id")
    label: str = Field(alias="label")
    permissions: List[FilteredPermission] = Field(default_factory=list, alias="permissions")
    parameters: List[Dict[str, Any]] = Field(default_factory=list, alias="parameters")
    filters: List[Any] = Field(default_factory=list, alias="filters")
    grants: List[Grant] = Field(default_factory=list, alias="grants")
    version: int = Field(default=0, alias="version")
    grantBased: bool = Field(default=True, alias="grantBased")


class Resource(BaseModel):
    id: str
    parentId: Optional[str]
    label: str
    type: str
    visibility: Optional[str] = None
    icon: Optional[str] = None


class DatabaseRole(BaseModel):
    id: str
    parameters: Dict[str, Any] = {}
    resources: List[Any] = []


class DatabaseTree(BaseModel):
    databaseId: str
    userId: str
    version: str
    label: str
    description: Optional[str] = None
    ownerRef: OwnerRef
    billingAccountId: int
    language: str
    originalLanguage: Optional[str] = None
    languages: List[str] = []
    role: DatabaseRole
    suspended: bool
    billingPlan: Dict[str, Any] | str = {}
    storage: str
    publishedTemplate: bool
    resources: List[Resource]
    grants: List[Grant]
    locks: List[Any] = []
    roles: List[Role]
    securityCategories: List[Dict[str, str]]


class FormFields(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(alias='@id')
    last_edit_time: float = Field(alias='@lastEditTime')


class OperationMetricConfigurationField(FormFields):
    sort_order: str = Field(alias='SORTORDER')
    data_form_prefix: str = Field(alias='DFORM.SYSPREFIX')
    data_form_id: str = Field(alias='DFORM.@id')
    order: int = Field(alias='REFORDER')
    shown_as: str = Field(alias='DISPLAY.@id')
    global_attachment_metrix: str = Field(alias='GLOBMETRIC.@id')
    reference_code_manual: Optional[str] = Field(alias='REFCODE_MAN')
    name: str = Field(alias='NAME')
    reference_code: str = Field(alias='REFCODE')
    field_name: str = Field(alias='CCODE')
    reference_label: str = Field(alias='REFLABEL')
    errors: Optional[str] = Field(alias='ERRS')


class OperationDataFormsField(FormFields):
    system_prefix: str = Field(alias='SYSPREFIX')
    entity_form_prefix: str = Field(alias='EFORM.SYSPREFIX')
    entity_form_id: str = Field(alias='EFORM.@id')
    composite_code: str = Field(alias='CCODE')


class Database(BaseModel):
    databaseId: str
    label: str
    description: Optional[str] = None


class TypeParameterLookupConfig(BaseModel):
    id: str
    formula: Optional[str] = None
    lookupLabel: Optional[str] = None


class TypeParameters(BaseModel):
    range: Optional[List[Dict[str, str]]] = None
    lookupConfigs: Optional[List[TypeParameterLookupConfig]] = None
    formula: Optional[str] = None
    units: Optional[str] = None
    aggregation: Optional[str] = None


class FieldTypeParametersItemsUpdateDTO(BaseModel):
    id: str = Field(alias="id")
    label: str = Field(alias="label")


class FieldTypeParametersUpdateDTO(BaseModel):
    model_config = {"populate_by_name": True}
    units: Optional[str] = Field(default=None, alias="units")
    input_mask: Optional[str] = Field(default=None, alias="inputMask")
    barcode: Optional[bool] = Field(default=None, alias="barcode")
    cardinality: Optional[str] = Field(default=None, alias="cardinality")
    range: Optional[List[Dict[str, str]]] = Field(default=None, alias="range")
    form_id: Optional[str] = Field(default=None, alias="formId")
    items: Optional[List[FieldTypeParametersItemsUpdateDTO]] = Field(default=None, alias="items")
    formula: Optional[str] = Field(default=None, alias="formula")
    prefix_formula: Optional[str] = Field(default=None, alias="prefixFormula")
    lookup_configs: Optional[List[TypeParameterLookupConfig]] = Field(default=None, alias="lookupConfigs")
    aggregation: Optional[str] = Field(default=None, alias="aggregation")


class SchemaFieldDTO(BaseModel):
    model_config = {"populate_by_name": True}
    id: str = Field(alias="id")
    code: str = Field(alias="code")
    label: str = Field(alias="label")
    description: Optional[str] = Field(default=None, alias="description")
    relevance_condition: Optional[str] = Field(default=None, alias="relevanceCondition")
    validation_condition: Optional[str] = Field(default=None, alias="validationCondition")
    data_entry_visible: bool = Field(default=True, alias="dataEntryVisible")
    table_visible: bool = Field(default=True, alias="tableVisible")
    required: bool = Field(alias="required")
    key: Optional[bool] = Field(default=False, alias="key")
    unique: Optional[bool] = Field(default=False, alias="unique")
    read_only: Optional[bool] = Field(default=False, alias="readOnly")
    default_value_formula: Optional[str] = Field(default=None, alias="defaultValueFormula")
    type: str = Field(alias="type")
    type_parameters: Optional[FieldTypeParametersUpdateDTO] = Field(default=None, alias="typeParameters")


class FormSchema(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: str
    schemaVersion: int
    databaseId: str
    parentFormId: Optional[str] = None
    label: str
    record_label_field_id: Optional[str] = Field(default=None, alias="recordLabelFieldId")
    elements: List[SchemaFieldDTO]


class AddDatabaseDTO(BaseModel):
    id: str
    label: str
    description: str
    templateId: str


class AddFormDTO(BaseModel):
    class FormResource(BaseModel):
        id: str
        type: str
        parentId: str
        label: str
        visibility: Optional[str] = None

    class FormClass(BaseModel):
        model_config = ConfigDict(populate_by_name=True)
        id: str
        label: str
        schemaVersion: int
        databaseId: str
        parentFormId: Optional[str] = None
        record_label_field_id: Optional[str] = Field(default=None, alias="recordLabelFieldId")
        elements: List[SchemaFieldDTO]

    formResource: FormResource
    formClass: FormClass


class UpdateDatabaseDTO(BaseModel):
    resourceUpdates: List[Resource] = Field(default_factory=list)
    resourceDeletions: List[str] = Field(default_factory=list)
    languageUpdates: List[str] = Field(default_factory=list)
    roleUpdates: List[Role] = Field(default_factory=list, alias="roleUpdates")
    originalLanguage: Optional[str] = Field(default=None, alias="originalLanguage")


class RecordUpdateDTO(BaseModel):
    form_id: str = Field(alias="formId")
    record_id: str = Field(alias="recordId")
    parent_record_id: Optional[str] = Field(default=None, alias="parentRecordId")
    deleted: Optional[bool] = Field(default=None, alias="deleted")
    fields: Dict[str, Any] = Field(alias="fields")

    model_config = {
        "populate_by_name": True,
    }


class UpdateRecordsDTO(BaseModel):
    changes: List[RecordUpdateDTO]


class DatabaseTranslationsID(BaseModel):
    database_id: str = Field(alias="databaseId")
    dictionary_id: str = Field(alias="dictionaryId")

    model_config = {
        "populate_by_name": True,
    }


class DatabaseTranslation(BaseModel):
    id: str = Field(alias="id")
    original: str = Field(alias="original")
    translated: str = Field(alias="translated")
    auto_translated: bool = Field(alias="autoTranslated")


class DatabaseTranslations(BaseModel):
    id: DatabaseTranslationsID = Field(alias="id")
    version: int = Field(alias="version")
    language: str = Field(alias="language")
    translated_strings: List[DatabaseTranslation] = Field(alias="translatedStrings")


class UpdateDatabaseTranslationsDTO(BaseModel):
    strings: List[DatabaseTranslation] = Field(alias="strings")


class DatabaseUser(BaseModel):
    database_id: str = Field(alias="databaseId")
    user_id: str = Field(alias="userId")
    name: str = Field(alias="name")
    email: str = Field(alias="email")
    role: DatabaseRole = Field(alias="role")
    version: int = Field(alias="version")
    invite_date: Optional[str] = Field(default=None, alias="inviteDate")
    delivery_status: Optional[str] = Field(default=None, alias="deliveryStatus")
    invite_accepted: Optional[bool] = Field(default=None, alias="inviteAccepted")
    locked: Optional[bool] = Field(default=None, alias="locked")
    user_license_type: Optional[str] = Field(default=None, alias="userLicenseType")
    last_login_date: Optional[str] = Field(default=None, alias="lastLoginDate")
    activation_status: Optional[str] = Field(default=None, alias="activationStatus")


class AddDatabaseUserDTO(BaseModel):
    email: str = Field(alias="email")
    name: str = Field(alias="name")
    locale: str = Field(alias="locale")
    role: DatabaseRole = Field(alias="role")
    grants: List[Any] = Field(default_factory=list, alias="grants")


class UpdateDatabaseUserRoleDTO(BaseModel):
    assignments: List[DatabaseRole] = Field(alias="assignments")
