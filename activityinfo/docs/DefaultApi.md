# client.DefaultApi

All URIs are relative to *https://www.activityinfo.org/resources*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_database**](DefaultApi.md#add_database) | **POST** /databases | Add Database
[**add_database_user**](DefaultApi.md#add_database_user) | **POST** /databases/{database_id}/users | Add Database User
[**add_form**](DefaultApi.md#add_form) | **POST** /databases/{database_id}/forms | Add Form
[**audit_database**](DefaultApi.md#audit_database) | **POST** /databases/{database_id}/audit | Audit Database
[**delete_database**](DefaultApi.md#delete_database) | **DELETE** /databases/{database_id} | Delete Database
[**delete_database_user**](DefaultApi.md#delete_database_user) | **DELETE** /databases/{database_id}/users/{user_id} | Delete Database User
[**get_database_translations**](DefaultApi.md#get_database_translations) | **GET** /databases/{database_id}/dictionary/database/{language_code} | Get Database Translations
[**get_database_tree**](DefaultApi.md#get_database_tree) | **GET** /databases/{database_id} | Get Database Tree
[**get_database_users**](DefaultApi.md#get_database_users) | **GET** /databases/{database_id}/users | Get Database Users
[**get_form_records**](DefaultApi.md#get_form_records) | **GET** /form/{form_id}/query | Get Form Records
[**get_form_schema**](DefaultApi.md#get_form_schema) | **GET** /form/{form_id}/schema | Get Form Schema
[**get_form_translations**](DefaultApi.md#get_form_translations) | **GET** /databases/{database_id}/dictionary/form/{form_id}/{language_code} | Get Form Translations
[**get_form_tree**](DefaultApi.md#get_form_tree) | **GET** /form/{form_id}/tree | Get Form Tree
[**get_job_status**](DefaultApi.md#get_job_status) | **GET** /jobs/{job_id} | Get Job Status
[**get_user_databases**](DefaultApi.md#get_user_databases) | **GET** /databases | Get User Databases
[**preflight_database_user**](DefaultApi.md#preflight_database_user) | **POST** /databases/{database_id}/users/preflight | Preflight Database User
[**query_rows**](DefaultApi.md#query_rows) | **POST** /query/rows | Query Rows
[**stage_import_direct**](DefaultApi.md#stage_import_direct) | **POST** /imports/stage/direct | Stage Import Direct
[**start_job**](DefaultApi.md#start_job) | **POST** /jobs | Start Job
[**update_database**](DefaultApi.md#update_database) | **POST** /databases/{database_id} | Update Database
[**update_database_translations**](DefaultApi.md#update_database_translations) | **POST** /databases/{database_id}/translations/{language_code} | Update Database Translations
[**update_database_user_role**](DefaultApi.md#update_database_user_role) | **POST** /databases/{database_id}/users/{user_id}/role | Update Database User Role
[**update_form_records**](DefaultApi.md#update_form_records) | **POST** /update | Update Form Records
[**update_form_schema**](DefaultApi.md#update_form_schema) | **POST** /form/{form_id}/schema | Update Form Schema
[**update_form_schema_translations**](DefaultApi.md#update_form_schema_translations) | **POST** /form/{form_id}/schema/translations/{language_code} | Update Form Schema Translations


# **add_database**
> Database add_database(add_database_request=add_database_request)

Add Database

Create a new database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.add_database_request import AddDatabaseRequest
from client.models.database import Database
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    add_database_request = client.AddDatabaseRequest() # AddDatabaseRequest |  (optional)

    try:
        # Add Database
        api_response = await api_instance.add_database(add_database_request=add_database_request)
        print("The response of DefaultApi->add_database:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->add_database: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **add_database_request** | [**AddDatabaseRequest**](AddDatabaseRequest.md)|  | [optional] 

### Return type

[**Database**](Database.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **add_database_user**
> User add_database_user(database_id, add_user_request=add_user_request)

Add Database User

Invite or add a new user to a database with a specific role.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.add_user_request import AddUserRequest
from client.models.user import User
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 
    add_user_request = client.AddUserRequest() # AddUserRequest |  (optional)

    try:
        # Add Database User
        api_response = await api_instance.add_database_user(database_id, add_user_request=add_user_request)
        print("The response of DefaultApi->add_database_user:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->add_database_user: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **add_user_request** | [**AddUserRequest**](AddUserRequest.md)|  | [optional] 

### Return type

[**User**](User.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **add_form**
> AddUpdateFormResponse add_form(database_id, add_form_request=add_form_request)

Add Form

Add a new form to a specific database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.add_form_request import AddFormRequest
from client.models.add_update_form_response import AddUpdateFormResponse
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 
    add_form_request = client.AddFormRequest() # AddFormRequest |  (optional)

    try:
        # Add Form
        api_response = await api_instance.add_form(database_id, add_form_request=add_form_request)
        print("The response of DefaultApi->add_form:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->add_form: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **add_form_request** | [**AddFormRequest**](AddFormRequest.md)|  | [optional] 

### Return type

[**AddUpdateFormResponse**](AddUpdateFormResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **audit_database**
> DatabaseAudit audit_database(database_id, audit_database_request=audit_database_request)

Audit Database

Gets entries from the database's audit log.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.audit_database_request import AuditDatabaseRequest
from client.models.database_audit import DatabaseAudit
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 
    audit_database_request = client.AuditDatabaseRequest() # AuditDatabaseRequest |  (optional)

    try:
        # Audit Database
        api_response = await api_instance.audit_database(database_id, audit_database_request=audit_database_request)
        print("The response of DefaultApi->audit_database:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->audit_database: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **audit_database_request** | [**AuditDatabaseRequest**](AuditDatabaseRequest.md)|  | [optional] 

### Return type

[**DatabaseAudit**](DatabaseAudit.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_database**
> str delete_database(database_id)

Delete Database

Deletes a database and all the forms and records contained therein. Only the owner of a database may delete a database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 

    try:
        # Delete Database
        api_response = await api_instance.delete_database(database_id)
        print("The response of DefaultApi->delete_database:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->delete_database: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 

### Return type

**str**

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: text/plain, application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | DELETED |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_database_user**
> str delete_database_user(database_id, user_id)

Delete Database User

Remove a user's access to a specific database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 
    user_id = 'user_id_example' # str | 

    try:
        # Delete Database User
        api_response = await api_instance.delete_database_user(database_id, user_id)
        print("The response of DefaultApi->delete_database_user:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->delete_database_user: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **user_id** | **str**|  | 

### Return type

**str**

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: text/plain, application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_database_translations**
> TranslationDictionary get_database_translations(database_id, language_code)

Get Database Translations

Fetch all database-level translations for a specific language.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.translation_dictionary import TranslationDictionary
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 
    language_code = 'language_code_example' # str | 

    try:
        # Get Database Translations
        api_response = await api_instance.get_database_translations(database_id, language_code)
        print("The response of DefaultApi->get_database_translations:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_database_translations: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **language_code** | **str**|  | 

### Return type

[**TranslationDictionary**](TranslationDictionary.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_database_tree**
> Database get_database_tree(database_id)

Get Database Tree

Fetch the full hierarchical tree structure of a database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.database import Database
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 

    try:
        # Get Database Tree
        api_response = await api_instance.get_database_tree(database_id)
        print("The response of DefaultApi->get_database_tree:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_database_tree: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 

### Return type

[**Database**](Database.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_database_users**
> List[User] get_database_users(database_id)

Get Database Users

List all users who have access to the specified database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.user import User
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 

    try:
        # Get Database Users
        api_response = await api_instance.get_database_users(database_id)
        print("The response of DefaultApi->get_database_users:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_database_users: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 

### Return type

[**List[User]**](User.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_form_records**
> List[Dict[str, object]] get_form_records(form_id)

Get Form Records

Query and retrieve all records for a specific form.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    form_id = 'form_id_example' # str | 

    try:
        # Get Form Records
        api_response = await api_instance.get_form_records(form_id)
        print("The response of DefaultApi->get_form_records:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_form_records: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **form_id** | **str**|  | 

### Return type

**List[Dict[str, object]]**

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_form_schema**
> FormSchema get_form_schema(form_id)

Get Form Schema

Retrieve the design/schema (fields, types, formulas) for a specific form.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.form_schema import FormSchema
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    form_id = 'form_id_example' # str | 

    try:
        # Get Form Schema
        api_response = await api_instance.get_form_schema(form_id)
        print("The response of DefaultApi->get_form_schema:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_form_schema: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **form_id** | **str**|  | 

### Return type

[**FormSchema**](FormSchema.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_form_translations**
> TranslationDictionary get_form_translations(database_id, form_id, language_code)

Get Form Translations

Retrieve all translated labels for a specific form and its fields.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.translation_dictionary import TranslationDictionary
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 
    form_id = 'form_id_example' # str | 
    language_code = 'language_code_example' # str | 

    try:
        # Get Form Translations
        api_response = await api_instance.get_form_translations(database_id, form_id, language_code)
        print("The response of DefaultApi->get_form_translations:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_form_translations: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **form_id** | **str**|  | 
 **language_code** | **str**|  | 

### Return type

[**TranslationDictionary**](TranslationDictionary.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_form_tree**
> FormTree get_form_tree(form_id)

Get Form Tree

Fetch the form and all its related forms (references).

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.form_tree import FormTree
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    form_id = 'form_id_example' # str | 

    try:
        # Get Form Tree
        api_response = await api_instance.get_form_tree(form_id)
        print("The response of DefaultApi->get_form_tree:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_form_tree: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **form_id** | **str**|  | 

### Return type

[**FormTree**](FormTree.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_job_status**
> JobStatus get_job_status(job_id)

Get Job Status

Retrieves the status of a long-running job

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.job_status import JobStatus
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    job_id = 'job_id_example' # str | 

    try:
        # Get Job Status
        api_response = await api_instance.get_job_status(job_id)
        print("The response of DefaultApi->get_job_status:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_job_status: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **job_id** | **str**|  | 

### Return type

[**JobStatus**](JobStatus.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_user_databases**
> List[GetDatabasesResponse] get_user_databases()

Get User Databases

Gets all the databases that the authenticated user owns, or that have been shared with the authenticated user.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.get_databases_response import GetDatabasesResponse
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)

    try:
        # Get User Databases
        api_response = await api_instance.get_user_databases()
        print("The response of DefaultApi->get_user_databases:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_user_databases: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**List[GetDatabasesResponse]**](GetDatabasesResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **preflight_database_user**
> UserPreflightResponse preflight_database_user(database_id, add_user_request=add_user_request)

Preflight Database User

Check the status of a user's email before adding them to a database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.add_user_request import AddUserRequest
from client.models.user_preflight_response import UserPreflightResponse
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 
    add_user_request = client.AddUserRequest() # AddUserRequest |  (optional)

    try:
        # Preflight Database User
        api_response = await api_instance.preflight_database_user(database_id, add_user_request=add_user_request)
        print("The response of DefaultApi->preflight_database_user:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->preflight_database_user: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **add_user_request** | [**AddUserRequest**](AddUserRequest.md)|  | [optional] 

### Return type

[**UserPreflightResponse**](UserPreflightResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **query_rows**
> List[Dict[str, object]] query_rows(query_rows_request)

Query Rows

Queries records as rows using column expressions, optional filter formula, filter sets, and sorting.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.query_rows_request import QueryRowsRequest
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    query_rows_request = client.QueryRowsRequest() # QueryRowsRequest | 

    try:
        # Query Rows
        api_response = await api_instance.query_rows(query_rows_request)
        print("The response of DefaultApi->query_rows:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->query_rows: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **query_rows_request** | [**QueryRowsRequest**](QueryRowsRequest.md)|  | 

### Return type

**List[Dict[str, object]]**

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **stage_import_direct**
> StageImportResponse stage_import_direct()

Stage Import Direct

Creates a direct upload staging slot and returns an upload URL and import id.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.stage_import_response import StageImportResponse
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)

    try:
        # Stage Import Direct
        api_response = await api_instance.stage_import_direct()
        print("The response of DefaultApi->stage_import_direct:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->stage_import_direct: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**StageImportResponse**](StageImportResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **start_job**
> JobStatus start_job(job_request=job_request)

Start Job

Starts a new long-running job

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.job_request import JobRequest
from client.models.job_status import JobStatus
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    job_request = client.JobRequest() # JobRequest |  (optional)

    try:
        # Start Job
        api_response = await api_instance.start_job(job_request=job_request)
        print("The response of DefaultApi->start_job:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->start_job: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **job_request** | [**JobRequest**](JobRequest.md)|  | [optional] 

### Return type

[**JobStatus**](JobStatus.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json, multipart/form-data
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_database**
> Database update_database(database_id, update_database_request=update_database_request)

Update Database

Modify database-level settings (e.g., enabled languages).

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.database import Database
from client.models.update_database_request import UpdateDatabaseRequest
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 
    update_database_request = client.UpdateDatabaseRequest() # UpdateDatabaseRequest |  (optional)

    try:
        # Update Database
        api_response = await api_instance.update_database(database_id, update_database_request=update_database_request)
        print("The response of DefaultApi->update_database:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->update_database: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **update_database_request** | [**UpdateDatabaseRequest**](UpdateDatabaseRequest.md)|  | [optional] 

### Return type

[**Database**](Database.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_database_translations**
> MessageResponse update_database_translations(database_id, language_code, update_translations_request=update_translations_request)

Update Database Translations

Update global translations for a specific database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.message_response import MessageResponse
from client.models.update_translations_request import UpdateTranslationsRequest
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 
    language_code = 'language_code_example' # str | 
    update_translations_request = client.UpdateTranslationsRequest() # UpdateTranslationsRequest |  (optional)

    try:
        # Update Database Translations
        api_response = await api_instance.update_database_translations(database_id, language_code, update_translations_request=update_translations_request)
        print("The response of DefaultApi->update_database_translations:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->update_database_translations: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **language_code** | **str**|  | 
 **update_translations_request** | [**UpdateTranslationsRequest**](UpdateTranslationsRequest.md)|  | [optional] 

### Return type

[**MessageResponse**](MessageResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_database_user_role**
> User update_database_user_role(database_id, user_id, update_user_request=update_user_request)

Update Database User Role

Modify the assigned role for an existing database user.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.update_user_request import UpdateUserRequest
from client.models.user import User
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    database_id = 'database_id_example' # str | 
    user_id = 'user_id_example' # str | 
    update_user_request = client.UpdateUserRequest() # UpdateUserRequest |  (optional)

    try:
        # Update Database User Role
        api_response = await api_instance.update_database_user_role(database_id, user_id, update_user_request=update_user_request)
        print("The response of DefaultApi->update_database_user_role:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->update_database_user_role: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **user_id** | **str**|  | 
 **update_user_request** | [**UpdateUserRequest**](UpdateUserRequest.md)|  | [optional] 

### Return type

[**User**](User.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_form_records**
> update_form_records(record_update_request=record_update_request)

Update Form Records

Bulk create, update, or delete records across one or more forms.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.record_update_request import RecordUpdateRequest
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    record_update_request = client.RecordUpdateRequest() # RecordUpdateRequest |  (optional)

    try:
        # Update Form Records
        await api_instance.update_form_records(record_update_request=record_update_request)
    except Exception as e:
        print("Exception when calling DefaultApi->update_form_records: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **record_update_request** | [**RecordUpdateRequest**](RecordUpdateRequest.md)|  | [optional] 

### Return type

void (empty response body)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_form_schema**
> AddUpdateFormResponse update_form_schema(form_id, form_schema=form_schema)

Update Form Schema

Push a modified schema back to the server to update a form's design.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.add_update_form_response import AddUpdateFormResponse
from client.models.form_schema import FormSchema
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    form_id = 'form_id_example' # str | 
    form_schema = client.FormSchema() # FormSchema |  (optional)

    try:
        # Update Form Schema
        api_response = await api_instance.update_form_schema(form_id, form_schema=form_schema)
        print("The response of DefaultApi->update_form_schema:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->update_form_schema: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **form_id** | **str**|  | 
 **form_schema** | [**FormSchema**](FormSchema.md)|  | [optional] 

### Return type

[**AddUpdateFormResponse**](AddUpdateFormResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_form_schema_translations**
> MessageResponse update_form_schema_translations(form_id, language_code, update_translations_request=update_translations_request)

Update Form Schema Translations

Apply new translations to a specific form and its fields.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.message_response import MessageResponse
from client.models.update_translations_request import UpdateTranslationsRequest
from client.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://www.activityinfo.org/resources
# See configuration.py for a list of all supported configuration parameters.
configuration = client.Configuration(
    host = "https://www.activityinfo.org/resources"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure Bearer authorization: bearerAuth
configuration = client.Configuration(
    access_token = os.environ["BEARER_TOKEN"]
)

# Enter a context with an instance of the API client
async with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = client.DefaultApi(api_client)
    form_id = 'form_id_example' # str | 
    language_code = 'language_code_example' # str | 
    update_translations_request = client.UpdateTranslationsRequest() # UpdateTranslationsRequest |  (optional)

    try:
        # Update Form Schema Translations
        api_response = await api_instance.update_form_schema_translations(form_id, language_code, update_translations_request=update_translations_request)
        print("The response of DefaultApi->update_form_schema_translations:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->update_form_schema_translations: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **form_id** | **str**|  | 
 **language_code** | **str**|  | 
 **update_translations_request** | [**UpdateTranslationsRequest**](UpdateTranslationsRequest.md)|  | [optional] 

### Return type

[**MessageResponse**](MessageResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful response |  -  |
**401** | Unauthorized - Invalid or missing API key |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

