import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from api.models import DatabaseTreeResourceType
from common import find_resource_by_prefix


@dataclass
class TranslationReport:
    translated_schema: dict
    form_id_replacements: int
    field_id_replacements: int
    unresolved_source_form_ids: List[str]
    unresolved_source_field_ids: List[str]


class SchemaIdTranslator:
    """
    Translates source schema form/field IDs to target database IDs.

    Mapping strategy:
    - forms: source form label -> target form id
    - fields: (source form label, source field code) -> target field id
    """

    FORMULA_KEYS = {
        "formula",
        "defaultValueFormula",
        "validationCondition",
        "relevanceCondition",
        "prefixFormula",
    }
    DIRECT_FORM_ID_KEYS = {"formId"}
    DIRECT_FIELD_ID_KEYS = {"recordLabelFieldId", "fieldId"}

    def __init__(self, client, source_form_id: str):
        self.client = client
        self.source_form_id = source_form_id
        self._source_schema_cache: Dict[str, object] = {}
        self._target_tree_cache: Dict[str, object] = {}
        self._target_form_schema_cache: Dict[Tuple[str, str], object] = {}
        self._indexed_all_source_forms = False

        source_schema = self._get_source_schema(source_form_id)
        self.source_db_id = source_schema.databaseId

        source_tree = self.client.api.get_database_tree(self.source_db_id)
        self.source_forms_by_id: Dict[str, str] = {
            res.id: res.label
            for res in source_tree.resources
            if res.type == DatabaseTreeResourceType.FORM
        }
        self.source_form_ids: Set[str] = set(self.source_forms_by_id.keys())

        self.source_field_meta_by_id: Dict[str, Tuple[str, Optional[str]]] = {}
        self._index_source_form_fields(source_form_id)

        # Preload directly referenced forms from source form to keep lookups fast
        # without loading every schema in the source database.
        for ref_form_id in self._extract_referenced_form_ids(source_schema):
            self._index_source_form_fields(ref_form_id)

    def _get_source_schema(self, form_id: str):
        cached = self._source_schema_cache.get(form_id)
        if cached is not None:
            return cached

        schema = self.client.api.get_form_schema(form_id)
        self._source_schema_cache[form_id] = schema
        return schema

    def _get_target_tree(self, target_db_id: str):
        cached = self._target_tree_cache.get(target_db_id)
        if cached is not None:
            return cached
        tree = self.client.api.get_database_tree(target_db_id)
        self._target_tree_cache[target_db_id] = tree
        return tree

    def _get_target_form_schema(self, target_db_id: str, form_id: str):
        cache_key = (target_db_id, form_id)
        cached = self._target_form_schema_cache.get(cache_key)
        if cached is not None:
            return cached
        schema = self.client.api.get_form_schema(form_id)
        self._target_form_schema_cache[cache_key] = schema
        return schema

    def _index_source_form_fields(self, source_form_id: str):
        form_label = self.source_forms_by_id.get(source_form_id)
        if not form_label:
            return
        try:
            form_schema = self._get_source_schema(source_form_id)
        except Exception:
            # Some forms may be inaccessible to the current token; skip gracefully.
            return

        for field in form_schema.elements:
            self.source_field_meta_by_id[field.id] = (form_label, field.code)

    def _index_all_source_forms(self):
        if self._indexed_all_source_forms:
            return
        for source_form_id in self.source_forms_by_id:
            self._index_source_form_fields(source_form_id)
        self._indexed_all_source_forms = True

    @staticmethod
    def _extract_referenced_form_ids(schema) -> Set[str]:
        referenced = set()
        for field in schema.elements:
            tp = field.type_parameters
            if not tp or not tp.range:
                continue
            for range_entry in tp.range:
                form_id = range_entry.get("formId")
                if isinstance(form_id, str):
                    referenced.add(form_id)
        return referenced

    @staticmethod
    def extract_id_like_tokens(text: str) -> Set[str]:
        return set(re.findall(r"\b[A-Za-z0-9]{12,}\b", text or ""))

    def _build_form_id_map(self, target_db_id: str) -> Dict[str, str]:
        target_tree = self._get_target_tree(target_db_id)
        target_forms = [
            res for res in target_tree.resources
            if res.type == DatabaseTreeResourceType.FORM
        ]
        
        form_id_map = {}
        for source_form_id, source_label in self.source_forms_by_id.items():
            target_form = find_resource_by_prefix(target_forms, source_label)
            if target_form and target_form.id != source_form_id:
                form_id_map[source_form_id] = target_form.id
        return form_id_map

    def _collect_referenced_ids(self, schema_dict: dict) -> Tuple[Set[str], Set[str]]:
        referenced_form_ids: Set[str] = set()
        referenced_field_ids: Set[str] = set()

        def walk(node, parent_key: Optional[str] = None):
            if isinstance(node, dict):
                for key, value in node.items():
                    if key in self.DIRECT_FORM_ID_KEYS and isinstance(value, str):
                        referenced_form_ids.add(value)
                    elif key in self.DIRECT_FIELD_ID_KEYS and isinstance(value, str):
                        referenced_field_ids.add(value)
                    walk(value, key)
                return

            if isinstance(node, list):
                for item in node:
                    walk(item, parent_key)
                return

            if isinstance(node, str) and parent_key in self.FORMULA_KEYS:
                for token in self.extract_id_like_tokens(node):
                    if token in self.source_form_ids:
                        referenced_form_ids.add(token)
                    else:
                        referenced_field_ids.add(token)

        walk(schema_dict)
        return referenced_form_ids, referenced_field_ids

    def _build_field_id_map(self, target_db_id: str, referenced_field_ids: Set[str]) -> Dict[str, str]:
        if not referenced_field_ids:
            return {}

        unresolved_source_meta = [fid for fid in referenced_field_ids if fid not in self.source_field_meta_by_id]
        if unresolved_source_meta:
            self._index_all_source_forms()

        needed_pairs: Set[Tuple[str, str]] = set()
        for source_field_id in referenced_field_ids:
            source_meta = self.source_field_meta_by_id.get(source_field_id)
            if not source_meta:
                continue
            source_form_label, source_field_code = source_meta
            if source_field_code:
                needed_pairs.add((source_form_label, source_field_code))

        if not needed_pairs:
            return {}

        target_tree = self._get_target_tree(target_db_id)
        target_forms = [
            res for res in target_tree.resources
            if res.type == DatabaseTreeResourceType.FORM
        ]

        needed_form_labels = {pair[0] for pair in needed_pairs}
        target_field_ids_by_pair: Dict[Tuple[str, str], str] = {}
        for form_label in needed_form_labels:
            target_form = find_resource_by_prefix(target_forms, form_label)
            if not target_form:
                continue
            try:
                target_schema = self._get_target_form_schema(target_db_id, target_form.id)
            except Exception:
                continue
            for field in target_schema.elements:
                if field.code:
                    target_field_ids_by_pair[(form_label, field.code)] = field.id

        field_id_map: Dict[str, str] = {}
        for source_field_id in referenced_field_ids:
            source_meta = self.source_field_meta_by_id.get(source_field_id)
            if not source_meta:
                continue
            source_form_label, source_field_code = source_meta
            if not source_field_code:
                continue
            target_field_id = target_field_ids_by_pair.get((source_form_label, source_field_code))
            if target_field_id and target_field_id != source_field_id:
                field_id_map[source_field_id] = target_field_id

        return field_id_map

    def translate_schema(self, schema_dict: dict, target_db_id: str) -> TranslationReport:
        if target_db_id == self.source_db_id:
            # Same-database apply: no translation required and no unresolveds should block.
            return TranslationReport(
                translated_schema=schema_dict,
                form_id_replacements=0,
                field_id_replacements=0,
                unresolved_source_form_ids=[],
                unresolved_source_field_ids=[],
            )

        referenced_form_ids, referenced_field_ids = self._collect_referenced_ids(schema_dict)
        form_id_map = self._build_form_id_map(target_db_id)
        field_id_map = self._build_field_id_map(target_db_id, referenced_field_ids)
        source_field_ids = set(self.source_field_meta_by_id.keys())

        unresolved_forms: Set[str] = set()
        unresolved_fields: Set[str] = set()
        form_replacements = 0
        field_replacements = 0

        def classify_unresolved(token: str):
            if token in referenced_form_ids and token not in form_id_map:
                unresolved_forms.add(token)
            elif token in source_field_ids and token not in field_id_map:
                unresolved_fields.add(token)

        def replace_formula_tokens(expr: str) -> str:
            nonlocal form_replacements, field_replacements

            tokens = self.extract_id_like_tokens(expr)
            for token in tokens:
                if token in self.source_form_ids or token in source_field_ids:
                    classify_unresolved(token)

            result = expr
            for source_id, target_id in form_id_map.items():
                result, hits = re.subn(
                    rf"(?<![A-Za-z0-9]){re.escape(source_id)}(?![A-Za-z0-9])",
                    target_id,
                    result
                )
                form_replacements += hits

            for source_id, target_id in field_id_map.items():
                result, hits = re.subn(
                    rf"(?<![A-Za-z0-9]){re.escape(source_id)}(?![A-Za-z0-9])",
                    target_id,
                    result
                )
                field_replacements += hits

            return result

        def walk(node, parent_key: Optional[str] = None):
            nonlocal form_replacements, field_replacements

            if isinstance(node, dict):
                updated = {}
                for key, value in node.items():
                    if key in self.DIRECT_FORM_ID_KEYS and isinstance(value, str):
                        if value in referenced_form_ids and value not in form_id_map:
                            unresolved_forms.add(value)
                        if value in form_id_map:
                            updated[key] = form_id_map[value]
                            form_replacements += 1
                        else:
                            updated[key] = value
                    elif key in self.DIRECT_FIELD_ID_KEYS and isinstance(value, str):
                        if value in source_field_ids and value not in field_id_map:
                            unresolved_fields.add(value)
                        if value in field_id_map:
                            updated[key] = field_id_map[value]
                            field_replacements += 1
                        else:
                            updated[key] = value
                    else:
                        updated[key] = walk(value, key)
                return updated

            if isinstance(node, list):
                return [walk(item, parent_key) for item in node]

            if isinstance(node, str) and parent_key in self.FORMULA_KEYS:
                return replace_formula_tokens(node)

            return node

        translated_schema = walk(schema_dict)
        return TranslationReport(
            translated_schema=translated_schema,
            form_id_replacements=form_replacements,
            field_id_replacements=field_replacements,
            unresolved_source_form_ids=sorted(unresolved_forms),
            unresolved_source_field_ids=sorted(unresolved_fields),
        )
