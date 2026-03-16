import os.path
from typing import Annotated, Set

import pandas as pd
import typer
from rich.table import Table

from api.models import AddDatabaseUserDTO, UpdateDatabaseUserRoleDTO, DatabaseRole
from utils import get_client, handle_api_errors, console

# Initialize a Typer sub-application for user management
app = typer.Typer(no_args_is_help=True)

# Define common user roles expected in the ActivityInfo system
USER_ROLES = ["Global Administrator", "CM Administrator", "CM Coordinator", "CM Partner"]


@app.command(help="Bulk add or update users in a database from a file.", no_args_is_help=True)
def add_bulk(
        target_database_id: Annotated[str, typer.Argument(help="The ID of the target database")],
        input_file_path: Annotated[str, typer.Argument(help="The path to the input file (CSV/XLSX)")],
        remove_users: Annotated[bool, typer.Option(help="Remove existing users missing from the input list")] = False,
        dry_run: Annotated[bool, typer.Option(help="Do not actually perform any changes")] = False,
        yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False
):
    """
    Synchronize database users with an external list provided in a CSV or Excel file.
    
    This command performs a 'diff' between the provided file and the current state of 
    the database users. It can add new users, update roles for existing users, and 
    optionally delete users not present in the input file.
    """
    client = get_client()

    # --- 1. Load and Validate Input File ---
    # Extract file extension to determine the appropriate loading method (CSV or Excel)
    filename, extension = os.path.splitext(input_file_path)
    with handle_api_errors(f"Could not read input file: {input_file_path}"):
        if extension == ".csv":
            data: pd.DataFrame = pd.read_csv(input_file_path)
        elif extension in [".xls", ".xlsx"]:
            data = pd.read_excel(input_file_path)
        else:
            raise typer.BadParameter(f"Unrecognized file extension: {extension}")

    # Standardize column names (lowercase and stripped) for easier matching
    data.columns = [col.lower().strip() for col in data.columns]

    # Check for mandatory columns: email (primary key), name (for new users), and role
    email_col = next((col for col in data.columns if "email" in col), None)
    if not email_col:
        console.print("[red]Error: Input file does not have an email column.[/red]")
        raise typer.Exit(code=1)

    if "name" not in data.columns or "role" not in data.columns:
        console.print("[red]Error: Input file must have 'name' and 'role' columns.[/red]")
        raise typer.Exit(code=1)

    # --- 2. Retrieve Target Database State ---
    # Fetch the database structure and current user list to perform comparison
    with handle_api_errors("Could not get target database information"):
        target_tree = client.api.get_database_tree(target_database_id)
        existing_users = client.api.get_database_users(target_database_id)

    # --- 3. Role Validation ---
    # Map role labels to their corresponding IDs from the target database
    existing_roles = {role.label: role for role in target_tree.roles}
    # Verify that the database actually has roles defined before proceeding
    matching_roles_count = len(set(USER_ROLES).intersection(existing_roles.keys()))
    if matching_roles_count == 0:
        console.print("[red]Error: Target database has no predefined roles.[/red]")
        raise typer.Exit(code=1)

    # --- 4. Comparison and Categorization ---
    known_emails: Set[str] = set()
    user_additions = []
    user_updates = []

    for _, row in data.iterrows():
        email = str(row[email_col]).strip().lower()
        name = str(row["name"]).strip()
        role_label = str(row["role"]).strip()

        # Skip entries where the requested role does not exist in the target database
        if role_label not in existing_roles:
            console.print(f"[yellow]Warning: Role '{role_label}' is not recognized for {email}. Skipping.[/yellow]")
            continue

        # Check if user already exists in the database
        existing_user = next((u for u in existing_users if u.email.lower() == email), None)
        if existing_user is None:
            user_additions.append((name, email, role_label))
        else:
            # Plan for an update if the user exists (even if role is same, we queue it for simplicity)
            user_updates.append((existing_user.user_id, email, role_label))
        known_emails.add(email)

    # Identify users to delete if the --remove-users flag is set
    user_deletes = []
    if remove_users:
        user_deletes = [u for u in existing_users if u.email.lower() not in known_emails]

    # --- 5. Display Proposed Changes ---
    if user_additions:
        add_table = Table(title="Users to ADD", title_style="green")
        add_table.add_column("Name")
        add_table.add_column("Email")
        add_table.add_column("Role")
        for name, email, role in user_additions:
            add_table.add_row(name, email, role)
        console.print(add_table)

    if user_updates:
        up_table = Table(title="Users to UPDATE", title_style="yellow")
        up_table.add_column("Email")
        up_table.add_column("New Role")
        for _, email, role in user_updates:
            up_table.add_row(email, role)
        console.print(up_table)

    if user_deletes:
        del_table = Table(title="Users to DELETE", title_style="red")
        del_table.add_column("Name")
        del_table.add_column("Email")
        for u in user_deletes:
            del_table.add_row(u.name, u.email)
        console.print(del_table)

    # Exit early for dry-run or if no changes are required
    if dry_run:
        console.print("\n[bold cyan]Dry run mode: No changes will be applied.[/bold cyan]")
        return

    if not (user_additions or user_updates or user_deletes):
        console.print("[green]No changes needed.[/green]")
        return

    # Prompt for final user confirmation unless --yes flag is provided
    if not yes and not typer.confirm("\nProceed with these changes?"):
        raise typer.Abort()

    # --- 6. Execution Phase ---
    with console.status("Applying changes...") as status:
        # Process Additions
        for name, email, role_label in user_additions:
            status.update(f"Adding user: {email}")
            with handle_api_errors(f"Could not add user {email}"):
                client.api.add_database_user(target_database_id, AddDatabaseUserDTO(
                    name=name, email=email, role=DatabaseRole(
                        id=existing_roles[role_label].id
                    ), grants=[], locale="en"
                ))

        # Process Role Updates
        for uid, email, role_label in user_updates:
            status.update(f"Updating user: {email}")
            with handle_api_errors(f"Could not update user {email}"):
                client.api.update_database_user_role(target_database_id, uid, UpdateDatabaseUserRoleDTO(
                    assignments=[DatabaseRole(id=existing_roles[role_label].id)],
                ))

        # Process Deletions
        for user in user_deletes:
            status.update(f"Deleting user: {user.email}")
            with handle_api_errors(f"Could not delete user {user.email}"):
                client.api.delete_database_user(target_database_id, user.user_id)

    console.print("[bold green]Bulk update completed successfully.[/bold green]")


if __name__ == "__main__":
    app()
