import os
import csv
import pytest
from typer.testing import CliRunner
from api.models import AddDatabaseDTO, UpdateDatabaseDTO, Role, FilteredPermission, Grant
from users import app, USER_ROLES
from cuid2 import Cuid

runner = CliRunner()

@pytest.fixture
def test_db(api_client):
    cuid = Cuid(length=18)
    db_id = cuid.generate()
    api_client.api.add_database(
        AddDatabaseDTO(id=db_id, label="User Test DB", description="Testing users", templateId="blank")
    )
    
    # Add roles as required by the script
    roles = []
    # Store roles by label for the test to reference later
    role_map = {}
    
    for role_label in USER_ROLES:
        role_id = cuid.generate() # MUST BE A VALID CUID
        role = Role(
            id=role_id,
            label=role_label,
            permissions=[
                FilteredPermission(operation="VIEW"),
                FilteredPermission(operation="DISCOVER")
            ],
            grants=[Grant(
                resourceId=db_id, 
                optional=False, 
                operations=[
                    FilteredPermission(operation="VIEW"),
                    FilteredPermission(operation="DISCOVER")
                ]
            )],
            version=0,
            grantBased=True
        )
        roles.append(role)
        role_map[role_label] = role
    
    # Register the roles in the database
    api_client.api.update_database(db_id, UpdateDatabaseDTO(
        roleUpdates=roles,
        originalLanguage="en"
    ))
    
    return db_id, role_map

def test_add_bulk_users(api_client, ai_setup, test_db, tmp_path):
    db_id, role_map = test_db
    base_url = ai_setup["url"]
    token = ai_setup["token"]
    os.environ["API_TOKEN"] = token
    os.environ["ACTIVITYINFO_BASE_URL"] = f"{base_url}/resources/"

    # 1. Create a CSV file
    csv_file = tmp_path / "users.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Email address", "Role"])
        writer.writerow(["User One", "user1@example.com", "Global Administrator"])
        writer.writerow(["User Two", "user2@example.com", "CM Coordinator"])

    # 2. Run add_bulk
    result = runner.invoke(app, [db_id, str(csv_file), "--yes"])
    
    if result.exit_code != 0:
        print(result.output)
        
    assert result.exit_code == 0
    assert "Bulk update completed successfully." in result.output

    # 3. Verify users added
    users = api_client.api.get_database_users(db_id)
    emails = [u.email.lower() for u in users]
    assert "user1@example.com" in emails
    assert "user2@example.com" in emails
    
    # 4. Update a user role
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Email address", "Role"])
        writer.writerow(["User One", "user1@example.com", "CM Administrator"]) # Changed role
        writer.writerow(["User Two", "user2@example.com", "CM Coordinator"])

    result = runner.invoke(app, [db_id, str(csv_file), "--yes"])
    if result.exit_code != 0:
        print(result.output)
    assert result.exit_code == 0
    
    users = api_client.api.get_database_users(db_id)
    u1 = next(u for u in users if u.email == "user1@example.com")
    assert u1.role.id == role_map["CM Administrator"].id

    # 5. Remove users
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Email address", "Role"])
        writer.writerow(["User One", "user1@example.com", "CM Administrator"])
        # User Two is missing

    result = runner.invoke(app, [db_id, str(csv_file), "--yes", "--remove-users"])
    assert result.exit_code == 0
    
    users = api_client.api.get_database_users(db_id)
    emails = [u.email.lower() for u in users]
    assert "user1@example.com" in emails
    assert "user2@example.com" not in emails
