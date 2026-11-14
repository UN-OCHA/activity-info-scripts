from enum import Enum


class FormSchemaElementType(str, Enum):
    ATTACHMENT = "attachment"
    CALCULATED = "calculated"
    DATE = "date"
    ENUMERATED = "enumerated"
    EPIWEEK = "epiweek"
    FORTNIGHT = "fortnight"
    FREE_TEXT = "FREE_TEXT"
    GEOPOINT = "geopoint"
    MONTH = "month"
    MULTISELECTREFERENCE = "multiselectreference"
    NARRATIVE = "NARRATIVE"
    QUANTITY = "quantity"
    REFERENCE = "reference"
    REVERSEREFERENCE = "reversereference"
    SECTION = "section"
    SERIAL = "serial"
    SUBFORM = "subform"

    def __str__(self) -> str:
        return str(self.value)
