from typing import List

from api.models import DatabaseTree, DatabaseTreeResourceType, Resource

DATA_FOLDER_PREFIXES = ["3", "4", "5", "6"]


def filter_data_forms(tree: DatabaseTree, folder_id: str) -> List[Resource]:
    top_level_folders = [
        res for res in tree.resources
        if res.type == DatabaseTreeResourceType.FOLDER
           and res.parentId == folder_id
           and res.label.startswith(tuple(DATA_FOLDER_PREFIXES))
    ]

    return [
        res for res in tree.resources
        if res.type == DatabaseTreeResourceType.FORM
           and res.parentId in [folder.id for folder in top_level_folders]
    ]
