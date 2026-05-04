# User


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**database_id** | **str** |  | 
**user_id** | **str** |  | 
**name** | **str** | The user&#39;s name, provided either when invited or by the user themselves after activating their account. | 
**email** | **str** | This user&#39;s email address. | 
**role** | [**UserRole**](UserRole.md) | This user&#39;s assigned role in the database. | 
**version** | **int** | A monotonically-increasing version number. This version is incremented each time a change is made to the users permissions. | [optional] 
**invite_date** | **date** | The date on which the user was invited to this database. | [optional] 
**invite_time** | **float** |  | [optional] 
**delivery_status** | [**AnyOf**](AnyOf.md) |  | [optional] 
**invite_accepted** | **bool** | True if the user has accepted the invitation to create an ActivityInfo account. | [optional] 
**locked** | **bool** | True if this user&#39;s account has been locked following too many failed authentication attempts. | [optional] 
**user_license_type** | [**AnyOf**](AnyOf.md) | The type of user license required for this user, in this database. | [optional] 
**last_login_date** | **date** | Date of last login on the platform. | [optional] 
**last_login_time** | **float** |  | [optional] 
**activation_status** | [**AnyOf**](AnyOf.md) | The user&#39;s current account status. | [optional] 

## Example

```python
from client.models.user import User

# TODO update the JSON string below
json = "{}"
# create an instance of User from a JSON string
user_instance = User.from_json(json)
# print the JSON string representation of the object
print(User.to_json())

# convert the object into a dict
user_dict = user_instance.to_dict()
# create an instance of User from a dict
user_from_dict = User.from_dict(user_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


