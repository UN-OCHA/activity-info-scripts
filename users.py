import os.path
import re
from typing import Annotated, Set, List, Dict, Any, Optional, cast
from datetime import datetime
import pandas as pd
import typer
import traceback

from rich.table import Table
from rich.console import Console


# deprecated
# from api.models import DatabaseRole

from api_client_utils import get_client, call_api

from activity_info_api_client.api.default import (
    get_database_tree,
    get_form_records,
    get_database_users,
    preflight_database_user,
    add_database_user,
    update_database_user_role,
    delete_database_user,
)
from activity_info_api_client.models import (
    AddUserRequest,
    UpdateUserRequest,
    UserRole,
    UserRoleParameters,
)

from common import find_resource_by_prefix

# Initialize a Typer sub-application for user management
app = typer.Typer(no_args_is_help=True)

# Initialize a rich Console for stylized CLI output
console = Console()

#Partner form properties
PARTNER_PROPERTIES = ["GLOBORG.ABBREV", "GLOBORG.HPCID", "NAME", "REFLABEL"]


# Standard email regex for pre-validation (spec says preflight API can also do this)
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")

class ProcessingError(Exception):
    pass

def get_record_id_by_property(records: List[Dict[str, Any]], properties: List[str], value: str) -> Optional[str]:
    """Find record ID in a form where the value matches one of the properties."""
    value_clean = value.strip().lower()
    for rec in records:
        for prop in properties:
            if str(rec.get(prop, "")).strip().lower() == value_clean:
                return rec.get("@id")
    return None

def add_users_to_database(
        target_database_id: str,
        data: pd.DataFrame,
        preflight_done: bool,
        update_user_roles: bool,
        update_user_parameters: bool,
        remove_users: bool,
        dry_run: bool,
        yes: bool,
        output_csv_path: str,
) -> tuple[str, bool]:

    """
    Returns:
        (database_name, preflight_done)
    """

    client = get_client()

    # --- 1. Retrieve Target Database State ---
    target_tree, api_error = call_api(
        get_database_tree.sync_detailed,
        database_id=target_database_id,
        client=client,
        message="Could not get target database tree",
        exit_on_error=False,
    )
    if api_error is not None:
        raise ProcessingError("")
    else:
        console.print(f"Database name is [bold]{target_tree.label}[/bold]")

    existing_users, api_error = call_api(
        get_database_users.sync_detailed,
        database_id=target_database_id,
        client=client,
        message="Could not get target database users",
        exit_on_error=False,
    )
    if api_error is not None:
        raise ProcessingError(target_tree.label)

    db_type = "HPC" if target_tree.label.startswith("HPC.tools") else "AIT" if target_tree.label.startswith(
        "AIT") else "Other"

    if db_type == "HPC":
        coordination_entities_form = find_resource_by_prefix(target_tree.resources, "1.1")
        partners_form = find_resource_by_prefix(target_tree.resources, "2.1")
        if not coordination_entities_form or not partners_form:
            console.print(f"[red]Error: Could not find forms 1.1 and/or 2.1.[/red]")
            raise ProcessingError(target_tree.label)

        coordination_entity_records, api_error = call_api(
            get_form_records.sync_detailed,
            form_id=coordination_entities_form.id,
            client=client,
            message=f"Could not retrieve coordination entity records",
            exit_on_error=False,
        )
        if api_error is not None:
            raise ProcessingError(target_tree.label)

        partner_records, api_error = call_api(
            get_form_records.sync_detailed,
            form_id=partners_form.id,
            client=client,
            message=f"Could not retrieve partner records",
            exit_on_error=False,
        )
        if api_error is not None:
            raise ProcessingError(target_tree.label)

    elif db_type == "AIT":
        partners_form = find_resource_by_prefix(target_tree.resources, "Partner")
        if not partners_form:
            console.print("[red]Error: Could not find Partner form.[/red]")
            raise ProcessingError(target_tree.label)

        partner_records, api_error = call_api(
            get_form_records.sync_detailed,
            form_id=partners_form.id,
            client=client,
            message=f"Could not retrieve partner records",
            exit_on_error=False,
        )
        if api_error is not None:
            raise ProcessingError(target_tree.label)

    else:
        console.print("[red]Error: database type not recognised as HPC.tools or AIT")
        raise ProcessingError(target_tree.label)

    # Map role labels to IDs from the target database
    db_role_label_to_id = {role.label.strip().lower(): role.id for role in target_tree.roles}

    # --- 5. Process Rows ---
    results: List[Dict[str, Any]] = []
    known_emails: Set[str] = set()

    user_additions: List[Dict[str, Any]] = []
    user_updates: List[Dict[str, Any]] = []
    user_deletions: List[Dict[str, Any]] = []

    # --- 6. Process input File to preflight Emails and match roles:
    with (console.status("Processing file...") as status):
        for idx, row in data.iterrows():
            status.update(f"Processing {idx + 1} of {len(data)}")

            # Spec: Stop if empty row reached
            if not row["email"] and not row["role"]:
                break
            elif not row["email"]:
                continue

            # Email preflight
            if preflight_done:
                preflight_error = row["preflight"]
            else:
                preflight, api_error = call_api(
                    preflight_database_user.sync_detailed,
                    database_id=target_database_id,
                    body=AddUserRequest(
                        email=row["email"],
                        name="",
                        locale="",
                        role=UserRole(id="default"),
                    ),
                    client=client,
                    message=f"Preflight failed for {row["email"]}",
                    exit_on_error=False,
                )
                if api_error is not None:
                    preflight_error = "Preflight API Error"
                elif not preflight.valid_email:
                    preflight_error = preflight.localized_error_message or "Invalid email"
                else:
                    preflight_error=""
                    data.loc[idx,"name"] = preflight.name if preflight.name else (row["name"] if row["name"] else row["email"])

            if preflight_error:
                data.loc[idx,"preflight"] = preflight_error
                results.append({**row["report"], "Action": "Ignored", "Message": preflight_error})
                continue

            # Role matching
            if row["role"].lower() not in db_role_label_to_id:
                results.append({**row["report"], "Action": "Ignored", "Message": f"Unknown role: {row["role"]}"})
                continue
            role_id = db_role_label_to_id[row["role"].lower()]

            # Handle Role Parameters
            if row["role"] == "CM Coordinator":
                if not row["cde"]:
                    results.append({**row["report"], "Action": "Ignored", "Message": "Missing cde"})
                    continue

                # Find record in 1.1 Coordination Entities
                rec_id = get_record_id_by_property([rec.to_dict() for rec in coordination_entity_records],
                                                   ["CCODE"], row["cde"])
                if not rec_id:
                    results.append({**row["report"], "Action": "Ignored", "Message": "Unknown cde"})
                    continue
                role_params = UserRoleParameters.from_dict({"cde": f"{coordination_entities_form.id}:{rec_id}"})

            elif row["role"] in ("CM Partner", "Partner Data Entry", "UN OIG",
                                    "Partner Validation", "Partner Country Representative/Designated Official"):
                if not row["org"]:
                    results.append({**row["report"], "Action": "Ignored", "Message": "Missing org"})
                    continue

                # Find record in Partners
                rec_id = get_record_id_by_property([rec.to_dict() for rec in partner_records],
                                                   PARTNER_PROPERTIES, row["org"])
                if not rec_id:
                    results.append({**row["report"], "Action": "Ignored", "Message": "Unknown org"})
                    continue
                role_params = UserRoleParameters.from_dict({"org": f"{partners_form.id}:{rec_id}"})

            else:
                role_params = UserRoleParameters()

            # User identification
            known_emails.add(row["email"])

            existing_user = next((u for u in existing_users if u.email.lower() == row["email"]), None)

            if not existing_user:
                user_additions.append({
                    "name": row["name"],
                    "email": row["email"],
                    "locale": row["locale"],
                    "role": UserRole(id=role_id, parameters=role_params),
                    "report_row": row["report"],
                })
            else:
                if update_user_roles or update_user_parameters:

                    # Check if role or params changed
                    changed = False
                    if existing_user.role.id != role_id:
                        if update_user_roles:
                            changed = True
                            update_message = "Existing user with different role"
                        else:
                            update_message = "Existing user with different role, do not update"
                    elif existing_user.role.id == role_id and existing_user.role.parameters != role_params:
                        if update_user_parameters:
                            changed = True
                            update_message = "Existing user with same role but different parameter"
                        else:
                            update_message = "Existing user with same role but different parameter, do not update"
                    else:
                        update_message = "Existing user with same role and parameter, no change"

                    if changed:
                        user_updates.append({
                            "user_id": existing_user.user_id,
                            "email": existing_user.email,
                            "role": UserRole(id=role_id, parameters=role_params),
                            "report_row": row["report"],
                            "update_message": update_message
                        })
                    else:
                        results.append({**row["report"], "Action": "Ignored", "Message": update_message})
                else:
                    results.append({**row["report"], "Action": "Ignored", "Message": "Existing user, do not update"})

    preflight_done = True

    # Identify Deletions
    if remove_users:
        db_role_id_to_label = {role.id: role.label.strip().lower() for role in target_tree.roles}

        for u in existing_users:
            if u.email.lower() not in known_emails:
                user_deletions.append({
                    "user_id": u.user_id,
                    "email": u.email,
                    "report_row": {
                        "Email": u.email,
                        "Role": db_role_id_to_label[u.role.id],
                        # TODO: fix this to return a parameter label (with a helper fn get_record_property_by_id)
                        "Parameter": ", ".join(f"{k}: {v}" for k, v in u.role.parameters.to_dict().items()),
                    },
                })

    # --- 6. Recap and Confirmation ---
    table = Table(title="Planned User Changes Summary")
    table.add_column("Email", style="cyan")
    table.add_column("Role", style="magenta")
    table.add_column("Parameter", style="dim")
    table.add_column("Action", style="bold")
    table.add_column("Message")

    # add existing results for all ignored values
    for res in results:
        table.add_row(*res.values(), style="grey70")

    for add in user_additions:
        table.add_row(*add['report_row'].values(), "Add", style="green")

    for up in user_updates:
        table.add_row(*up['report_row'].values(), "Update", up['update_message'], style="yellow")

    for dele in user_deletions:
        table.add_row(*dele['report_row'].values(), "Delete", style="red")

    console.print(table)

    output_csv = (output_csv_path + "users_add-bulk_" +
                  target_tree.label.replace(":", "").replace(" ", "_").replace("__", "_") +
                  "_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S")) + ".csv"
    output_dirpath = os.path.dirname(output_csv)
    if output_dirpath:
        os.makedirs(output_dirpath, exist_ok=True)

    if not (user_additions or user_updates or user_deletions):
        console.print("[green]No changes needed.[/green]")
        report_df = pd.DataFrame(results)
        report_df.to_csv(output_csv, index=False)
        console.print(f"[bold green]Action report written to {output_csv}[/bold green]")
        return target_tree.label, preflight_done

    if dry_run:
        console.print(
            f"\n[bold cyan]Dry run mode: {len(user_additions)} additions, {len(user_updates)} updates, {len(user_deletions)} deletions planned.[/bold cyan]")
        return target_tree.label, preflight_done

    if not yes and not typer.confirm("\nProceed with these changes?"):
        console.print(f"[bold green]Process aborted without changes, no action report written.[/bold green]")
        return target_tree.label, preflight_done

    # --- 7. Execution ---
    with console.status("Applying changes...") as status:
        for add in user_additions:
            status.update(f"Adding user: {add['email']}")

            api_response, api_error = call_api(
                add_database_user.sync_detailed,
                database_id=target_database_id,
                body=AddUserRequest(
                    name=cast(str, add['name']),
                    email=cast(str, add['email']),
                    locale=cast(str, add['locale']),
                    role=cast(UserRole, add['role']),
                    grants=[]
                ),
                client=client,
                message=f"Could not add user {add['email']}",
                exit_on_error=False,
            )
            if api_error is None:
                results.append({**add['report_row'], "Action": "Added", "Message": ""})
            else:
                results.append({**add['report_row'], "Action": "Failed to Add", "Message": api_error})

        for up in user_updates:
            status.update(f"Updating user: {up['email']}")

            api_response, api_error = call_api(
                update_database_user_role.sync_detailed,
                database_id=target_database_id,
                user_id=up['user_id'],
                body=UpdateUserRequest(
                    assignments=[cast(UserRole, up['role'])],
                ),
                client=client,
                message=f"Could not update user {up['email']}",
                exit_on_error=False,
            )
            if api_error is None:
                results.append({**up['report_row'], "Action": "Updated", "Message": ""})
            else:
                results.append({**up['report_row'], "Action": "Failed to Update", "Message": api_error})

        for dele in user_deletions:
            status.update(f"Deleting user: {dele['email']}")

            api_response, api_error = call_api(
                delete_database_user.sync_detailed,
                database_id=target_database_id,
                user_id=dele['user_id'],
                client=client,
                message=f"Could not delete user {dele['email']}",
                exit_on_error=False,
            )
            if api_error is None:
                results.append({**dele['report_row'], "Action": "Deleted", "Message": ""})
            else:
                results.append({**dele['report_row'], "Action": "Failed to Delete", "Message": api_error})

    # --- 5. Output Report ---
    report_df = pd.DataFrame(results)
    report_df.to_csv(output_csv, index=False)
    console.print(f"[bold green]Sync completed. Action report written to {output_csv}[/bold green]")

    return target_tree.label, preflight_done

@app.command(help="Bulk add or update users in one or more databases from a CSV file.", no_args_is_help=True)
def add_bulk(
        target_database_ids: Annotated[str, typer.Argument(help="The ID(s) of the target database in a comma separated string")],
        input_file_path: Annotated[str, typer.Argument(help="The path to the input CSV file")],
        update_user_roles: Annotated[bool, typer.Option(help="Update existing users if role is different on input list")] = True,
        update_user_parameters: Annotated[bool, typer.Option(help="Update existing users if role is same but parameters are different on input list")] = True,
        remove_users: Annotated[bool, typer.Option(help="Remove existing users missing from the input list")] = False,
        dry_run: Annotated[bool, typer.Option(help="Do not actually perform any changes")] = False,
        yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
        output_csv_path: Annotated[
            str, typer.Option("--output", "-o", help="Path to folder for action reports")] = "output/"
):
    """
    Synchronize database users with an external list provided in a CSV file.
    Implements SPPB-43 requirements including parameter matching and preflight checks.
    """

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
    if email_col:
        data.rename(columns={email_col: "email"}, inplace=True)
    else:
        console.print("[red]Error: Input file must have 'Email' or 'Email address' column.[/red]")
        raise typer.Exit(code=1)

    if "role" not in data.columns:
        console.print("[red]Error: Input file must have 'Role' column.[/red]")
        raise typer.Exit(code=1)

    # --- 2. Pre-process Input File to add missing columns and populate missing row data:

    #TODO: modify entire script to have the parameters not hardcoded but looked up from the UserRoleParameters:
    #TODO: match parameterId or label against CSV col name, and iterate through form field labels/codes to look up table values.
    #TODO: or just have one CSV col called Parameter (or Parameter1, Parameter2).
    defaults = {
        "email": "",
        "role": "",
        "name": "",
        "locale": "en",
        "cde":"",
        "org":"",
        "preflight": "",
    }
    for col, default in defaults.items():
        if col not in data.columns:
            data[col] = default
        else:
            data[col] = data[col].fillna(default).astype(str).str.strip().replace("", default)

    data["email"] = data["email"].str.lower()
    data["locale"] = data["locale"].str.lower()

    data["report"] = data.apply(
        lambda row: {
            "Email": row["email"],
            "Role": row["role"],
            "Parameter": "cde: " + row["cde"] if row["cde"] != "" else "org: " + row["org"] if row["org"] != "" else "",
        },
        axis=1
    )

    # --- 3. Split database ids:
    target_database_id_list = [id.strip() for id in target_database_ids.split(",") if id.strip()]
    if len(target_database_id_list) == 0:
        console.print(f"[red]No database(s) specified[/red]")
        raise typer.Exit(code=1)

    # --- 4. Iterate through databases to run the databases-specific process:
    preflight_done = False

    for target_database_id in target_database_id_list:

        console.print(f"[bold]Starting processing for database {target_database_id}.[/bold]")

        try:
            target_database_label, preflight_done = add_users_to_database(target_database_id, data, preflight_done,
                                                                          update_user_roles, update_user_parameters,
                                                                          remove_users, dry_run, yes, output_csv_path)
            console.print(f"[green][bold]Successfully processed database {target_database_id} ({target_database_label}).[/bold][/green]")

        except ProcessingError as e:
            if str(e):
                console.print(f"[red][bold]Aborting processing for database {target_database_id} ({e}).[/bold][/red]")
            else:
                console.print(f"[red][bold]Aborting processing for database {target_database_id}.[/bold][/red]")

        except Exception as e:
            traceback.print_exc()


    console.print("[bold]All databases processed.[/bold]")


if __name__ == "__main__":
    app()
