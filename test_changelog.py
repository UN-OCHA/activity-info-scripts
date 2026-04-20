from datetime import datetime

from rich.text import Text

from api.models import DatabaseAuditRequestDTO, DatabaseUpdateType
from utils import get_client, console

if __name__ == "__main__":
    client = get_client()
    now_ms = int(datetime.now().timestamp() * 1000)
    one_week_ago_ms = now_ms - (7 * 24 * 3600 * 1000)
    res = client.api.audit_database(database_id="c95hy2sml6az15b6on", dto=DatabaseAuditRequestDTO(
        typeFilter=[DatabaseUpdateType.FOLDER, DatabaseUpdateType.FORM, DatabaseUpdateType.RECORD],
        startTime=now_ms,
        endTime=one_week_ago_ms
    ))
    for el in res.events:
        text = Text(f"[{datetime.fromtimestamp(el.time / 1000)}] {el.user.name} {el.description}")
        if "Deleted" in el.description:
            text.stylize("red", True)
        elif "Renamed" in el.description or "Updated" in el.description:
            text.stylize("dark_orange", True)
        elif "Added" in el.description:
            text.stylize("green", True)
        console.print(text)
