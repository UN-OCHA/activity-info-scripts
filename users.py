import os.path
import re
from typing import Annotated, Set, List, Dict, Any, Optional, cast

import pandas as pd
import typer

from rich.table import Table
from api.models import (
    AddDatabaseUserDTO,
    UpdateDatabaseUserRoleDTO,
    DatabaseRole,
    UserPreflightDTO
)
from utils import get_client, handle_api_errors, console

# Initialize a Typer sub-application for user management
app = typer.Typer(no_args_is_help=True)

# SPPB roles as per spec
USER_ROLES = ["Global Administrator", "CM Administrator", "CM Coordinator", "CM Partner"]

# Form IDs for parameters as per spec
# FORM_ID_COORDINATION_ENTITIES = "c9mpkvrml3u24bz1a9"
# FORM_ID_PARTNERS = "cezl1y0mms4u30z1poo"

# Standard email regex for pre-validation (spec says preflight API can also do this)
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")


def get_record_id_by_ccode(records: List[Dict[str, Any]], ccode: str) -> Optional[str]:
    """Find record ID in a form where CCODE matches."""
    ccode_clean = ccode.strip().lower()
    for rec in records:
        if str(rec.get("CCODE", "")).strip().lower() == ccode_clean:
            return cast(str, rec.get("@id"))
    return None


def get_record_id_by_org_abbrev(records: List[Dict[str, Any]],abbrev: str) -> Optional[str]:
    """Find record ID in form 2.1 Partners where GLOBORG.ABBREV matches."""
    abbrev_clean = abbrev.strip().lower()
    for rec in records:
        # Based on spec "GLOBORG.ABBREV referenced field"
        if str(rec.get("GLOBORG.ABBREV", "")).strip().lower() == abbrev_clean:
            return cast(str, rec.get("@id"))
    return None


@app.command(help="Bulk add or update users in a database from a CSV file.", no_args_is_help=True)
def add_bulk(
        target_database_id: Annotated[str, typer.Argument(help="The ID of the target database")],
        input_file_path: Annotated[str, typer.Argument(help="The path to the input CSV file")],
        remove_users: Annotated[bool, typer.Option(help="Remove existing users missing from the input list")] = False,
        dry_run: Annotated[bool, typer.Option(help="Do not actually perform any changes")] = False,
        yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
        output_csv: Annotated[
            str, typer.Option("--output", "-o", help="Path to output status CSV file")] = "user_sync_status.csv"
):
    """
    Synchronize database users with an external list provided in a CSV file.
    Implements SPPB-43 requirements including parameter matching and preflight checks.
    """
    client = get_client()

    # --- 1. Load and Validate Input File ---
    if not os.path.exists(input_file_path):
        console.print(f"[red]Error: File not found: {input_file_path}[/red]")
        raise typer.Exit(code=1)

    try:
        # Spec says CSV format
        data: pd.DataFrame = pd.read_csv(input_file_path)
    except Exception as e:
        console.print(f"[red]Error reading CSV: {e}[/red]")
        raise typer.Exit(code=1)

    # Standardize column names
    data.columns = [str(col).lower().strip() for col in data.columns]

    # Check for mandatory columns: Email and Role
    email_col = next((col for col in data.columns if col in ["email", "email address"]), None)
    if not email_col:
        console.print("[red]Error: Input file must have 'Email' or 'Email address' column.[/red]")
        raise typer.Exit(code=1)

    if "role" not in data.columns:
        console.print("[red]Error: Input file must have 'Role' column.[/red]")
        raise typer.Exit(code=1)

    # --- 2. Retrieve Target Database State ---
    with handle_api_errors("Could not get target database information"):
        target_tree = client.api.get_database_tree(target_database_id)
        existing_users = client.api.get_database_users(target_database_id)

    coordination_entities_form_id = next(
        res.id for res in target_tree.resources if res.label.startswith("1.1")
    )
    partners_form_id = next(
        res.id for res in target_tree.resources if res.label.startswith("2.1")
    )
    if not coordination_entities_form_id or not partners_form_id:
        console.print("[red]Error: Could not find forms 1.1 and/or 2.1.[/red]")
        raise typer.Exit(code=1)

    with handle_api_errors("Could not retrieve coordination or partner records"):
        coordination_entity_records = client.api.get_form(coordination_entities_form_id)
        partner_records = client.api.get_form(partners_form_id)

    # Map role labels to IDs from the target database
    db_roles = {role.label.strip().lower(): role for role in target_tree.roles}

    # --- 3. Process Rows ---
    results: List[Dict[str, Any]] = []
    known_emails: Set[str] = set()

    user_additions: List[Dict[str, Any]] = []
    user_updates: List[Dict[str, Any]] = []

    for idx, row in data.iterrows():

        print(f"Processing {idx} of {len(data)}")

        # Spec: Stop if empty row reached
        if pd.isna(row[email_col]) and pd.isna(row.get("role")):
            break

        email_raw = str(row[email_col]).strip() if not pd.isna(row[email_col]) else ""
        role_label_raw = str(row["role"]).strip() if not pd.isna(row["role"]) else ""
        name_raw = str(row.get("name", "")).strip() if not pd.isna(row.get("name")) else ""
        lang_raw = str(row.get("language", "en")).strip() if not pd.isna(row.get("language")) else "en"
        cde_raw = str(row.get("cde", "")).strip() if not pd.isna(row.get("cde")) else ""
        org_raw = str(row.get("org", "")).strip() if not pd.isna(row.get("org")) else ""

        if not email_raw:
            continue

        # Email preflight
        with handle_api_errors(f"Preflight failed for {email_raw}"):
            preflight = client.api.user_preflight(target_database_id, UserPreflightDTO(email=email_raw))

        if not preflight.valid_email:
            results.append(
                {"Email": email_raw, "Role": role_label_raw, "cde": cde_raw, "org": org_raw, "Status": "Ignored",
                 "Message": preflight.localized_error_message or "Invalid email"})
            continue

        # Role matching
        role_key = role_label_raw.lower()
        if role_key not in db_roles:
            results.append(
                {"Email": email_raw, "Role": role_label_raw, "cde": cde_raw, "org": org_raw, "Status": "Ignored",
                 "Message": f"Unknown role: {role_label_raw}"})
            continue

        target_role = db_roles[role_key]
        role_id = target_role.id
        role_params: Dict[str, Any] = {}

        # Handle Role Parameters
        if role_label_raw == "CM Coordinator":
            if not cde_raw:
                results.append(
                    {"Email": email_raw, "Role": role_label_raw, "cde": cde_raw, "org": org_raw, "Status": "Ignored",
                     "Message": "Missing cde for CM Coordinator"})
                continue

            # Find record in 1.1 Coordination Entities
            rec_id = get_record_id_by_ccode(coordination_entity_records, cde_raw)
            if not rec_id:
                results.append(
                    {"Email": email_raw, "Role": role_label_raw, "cde": cde_raw, "org": org_raw, "Status": "Ignored",
                     "Message": "Unknown cde"})
                continue
            role_params = {"cde": f"{coordination_entities_form_id}:{rec_id}"}

        elif role_label_raw == "CM Partner":
            if not org_raw:
                results.append(
                    {"Email": email_raw, "Role": role_label_raw, "cde": cde_raw, "org": org_raw, "Status": "Ignored",
                     "Message": "Missing org for CM Partner"})
                continue

            # Find record in 2.1 Partners
            rec_id = get_record_id_by_org_abbrev(partner_records, org_raw)
            if not rec_id:
                results.append(
                    {"Email": email_raw, "Role": role_label_raw, "cde": cde_raw, "org": org_raw, "Status": "Ignored",
                     "Message": "Unknown org"})
                continue
            role_params = {"org": f"{partners_form_id}:{rec_id}"}

        # User identification
        email_clean = email_raw.lower()
        known_emails.add(email_clean)

        existing_user = next((u for u in existing_users if u.email.lower() == email_clean), None)

        # Name fallback
        final_name = name_raw if name_raw else (preflight.name if preflight.name else email_raw)
        final_lang = lang_raw if lang_raw else "en"

        if not existing_user:
            user_additions.append({
                "name": final_name,
                "email": email_clean,
                "locale": final_lang,
                "role": DatabaseRole(id=role_id, parameters=role_params),
                "orig_row": {"Email": email_raw, "Role": role_label_raw, "cde": cde_raw, "org": org_raw}
            })
        else:
            # Check if role or params changed
            changed = False
            if existing_user.role.id != role_id:
                changed = True
            elif existing_user.role.parameters != role_params:
                changed = True

            if changed:
                user_updates.append({
                    "user_id": existing_user.user_id,
                    "email": email_clean,
                    "role": DatabaseRole(id=role_id, parameters=role_params),
                    "orig_row": {"Email": email_raw, "Role": role_label_raw, "cde": cde_raw, "org": org_raw}
                })
            else:
                results.append(
                    {"Email": email_raw, "Role": role_label_raw, "cde": cde_raw, "org": org_raw, "Status": "Unchanged",
                     "Message": ""})

    # Identify Deletions
    user_deletions = []
    if remove_users:
        for u in existing_users:
            if u.email.lower() not in known_emails:
                user_deletions.append(u)

    # --- 4. Recap and Confirmation ---
    role_id_to_label = {r.id: r.label for r in target_tree.roles}

    if user_additions or user_updates or user_deletions:
        table = Table(title="Planned User Changes Summary")
        table.add_column("Action", style="bold")
        table.add_column("Email", style="cyan")
        table.add_column("Role", style="magenta")
        table.add_column("Parameters", style="dim")

        for add in user_additions:
            role_obj = cast(DatabaseRole, add['role'])
            role_label = role_id_to_label.get(role_obj.id, role_obj.id)
            params = str(role_obj.parameters) if role_obj.parameters else "-"
            table.add_row("Add", add['email'], role_label, params, style="green")

        for up in user_updates:
            role_obj = cast(DatabaseRole, up['role'])
            role_label = role_id_to_label.get(role_obj.id, role_obj.id)
            params = str(role_obj.parameters) if role_obj.parameters else "-"
            table.add_row("Modify", up['email'], role_label, params, style="yellow")

        for dele in user_deletions:
            role_label = role_id_to_label.get(dele.role.id, dele.role.id)
            params = str(dele.role.parameters) if dele.role.parameters else "-"
            table.add_row("Delete", dele.email, role_label, params, style="red")

        console.print(table)
    else:
        console.print("[green]No changes needed.[/green]")
        return

    if dry_run:
        console.print(
            f"\n[bold cyan]Dry run mode: {len(user_additions)} additions, {len(user_updates)} updates, {len(user_deletions)} deletions planned.[/bold cyan]")
        return

    if not yes and not typer.confirm("\nProceed with these changes?"):
        raise typer.Abort()

    # --- 5. Execution ---
    with console.status("Applying changes...") as status:
        for add in user_additions:
            status.update(f"Adding user: {add['email']}")
            with handle_api_errors(f"Could not add user {add['email']}"):
                client.api.add_database_user(target_database_id, AddDatabaseUserDTO(
                    name=cast(str, add['name']),
                    email=cast(str, add['email']),
                    locale=cast(str, add['locale']),
                    role=cast(DatabaseRole, add['role']),
                    grants=[]
                ))
                results.append({**add['orig_row'], "Status": "Added", "Message": ""})

        for up in user_updates:
            status.update(f"Updating user: {up['email']}")
            with handle_api_errors(f"Could not update user {up['email']}"):
                client.api.update_database_user_role(target_database_id, cast(str, up['user_id']),
                                                     UpdateDatabaseUserRoleDTO(
                                                         assignments=[cast(DatabaseRole, up['role'])]
                                                     ))
                results.append({**up['orig_row'], "Status": "Modified", "Message": ""})

        for dele in user_deletions:
            status.update(f"Deleting user: {dele.email}")
            with handle_api_errors(f"Could not delete user {dele.email}"):
                # Capture current role for reporting
                role_lbl = next((r.label for r in target_tree.roles if r.id == dele.role.id), dele.role.id)
                client.api.delete_database_user(target_database_id, dele.user_id)
                results.append({"Email": dele.email, "Role": role_lbl, "cde": "", "org": "", "Status": "Deleted",
                                "Message": ""})

    # --- 5. Output Report ---
    report_df = pd.DataFrame(results)
    report_df.to_csv(output_csv, index=False)
    console.print(f"[bold green]Sync completed. Status report written to {output_csv}[/bold green]")


if __name__ == "__main__":
    app()
