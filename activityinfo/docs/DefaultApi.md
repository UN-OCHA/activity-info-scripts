# client.DefaultApi

All URIs are relative to *https://www.activityinfo.org/resources*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_database_post**](DefaultApi.md#add_database_post) | **POST** /databases | Create a new database.
[**add_database_user_post**](DefaultApi.md#add_database_user_post) | **POST** /databases/{database_id}/users | Invite or add a new user to a database with a specific role.
[**add_form_post**](DefaultApi.md#add_form_post) | **POST** /databases/{database_id}/forms | Add a new form to a specific database.
[**delete_database_user_delete**](DefaultApi.md#delete_database_user_delete) | **DELETE** /databases/{database_id}/users/{user_id} | Remove a user&#39;s access to a specific database.
[**get_database_translations_get**](DefaultApi.md#get_database_translations_get) | **GET** /databases/{database_id}/dictionary/database/{language_code} | Fetch all database-level translations for a specific language.
[**get_database_tree_get**](DefaultApi.md#get_database_tree_get) | **GET** /databases/{database_id} | Fetch the full hierarchical tree structure of a database.
[**get_database_users_get**](DefaultApi.md#get_database_users_get) | **GET** /databases/{database_id}/users | List all users who have access to the specified database.
[**get_form_get**](DefaultApi.md#get_form_get) | **GET** /form/{form_id}/query | Query and retrieve all records for a specific form.
[**get_form_schema_get**](DefaultApi.md#get_form_schema_get) | **GET** /form/{form_id}/schema | Retrieve the design/schema (fields, types, formulas) for a specific form.
[**get_form_translations_get**](DefaultApi.md#get_form_translations_get) | **GET** /databases/{database_id}/dictionary/form/{form_id}/{language_code} | Retrieve all translated labels for a specific form and its fields.
[**get_form_tree_get**](DefaultApi.md#get_form_tree_get) | **GET** /form/{form_id}/tree | Fetch the form and all its related forms (references).
[**get_user_databases_get**](DefaultApi.md#get_user_databases_get) | **GET** /databases | List all databases the authenticated user has access to.
[**query_rows_post**](DefaultApi.md#query_rows_post) | **POST** /query/rows | Query form rows with formulas, filters, and sorting.
[**update_database_post**](DefaultApi.md#update_database_post) | **POST** /databases/{database_id} | Modify database-level settings (e.g., enabled languages).
[**update_database_translations_post**](DefaultApi.md#update_database_translations_post) | **POST** /databases/{database_id}/translations/{language_code} | Update global translations for a specific database.
[**update_database_user_role_post**](DefaultApi.md#update_database_user_role_post) | **POST** /databases/{database_id}/users/{user_id}/role | Modify the assigned role for an existing database user.
[**update_form_records_post**](DefaultApi.md#update_form_records_post) | **POST** /update | Bulk create, update, or delete records across one or more forms.
[**update_form_schema_post**](DefaultApi.md#update_form_schema_post) | **POST** /form/{form_id}/schema | Push a modified schema back to the server to update a form&#39;s design.
[**update_form_translations_post**](DefaultApi.md#update_form_translations_post) | **POST** /databases/translations/{database_id}/form/{form_id}/{language_code} | Apply new translations to a specific form and its fields.
[**user_preflight_post**](DefaultApi.md#user_preflight_post) | **POST** /databases/{database_id}/users/preflight | Check the status of a user&#39;s email before adding them to a database.


# **add_database_post**
> object add_database_post(add_database_dto=add_database_dto)

Create a new database.

Create a new database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.add_database_dto import AddDatabaseDTO
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
    add_database_dto = client.AddDatabaseDTO() # AddDatabaseDTO |  (optional)

    try:
        # Create a new database.
        api_response = await api_instance.add_database_post(add_database_dto=add_database_dto)
        print("The response of DefaultApi->add_database_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->add_database_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **add_database_dto** | [**AddDatabaseDTO**](AddDatabaseDTO.md)|  | [optional] 

### Return type

**object**

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

# **add_database_user_post**
> object add_database_user_post(database_id, add_database_user_dto=add_database_user_dto)

Invite or add a new user to a database with a specific role.

Invite or add a new user to a database with a specific role.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.add_database_user_dto import AddDatabaseUserDTO
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
    add_database_user_dto = client.AddDatabaseUserDTO() # AddDatabaseUserDTO |  (optional)

    try:
        # Invite or add a new user to a database with a specific role.
        api_response = await api_instance.add_database_user_post(database_id, add_database_user_dto=add_database_user_dto)
        print("The response of DefaultApi->add_database_user_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->add_database_user_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **add_database_user_dto** | [**AddDatabaseUserDTO**](AddDatabaseUserDTO.md)|  | [optional] 

### Return type

**object**

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

# **add_form_post**
> object add_form_post(database_id, add_form_dto=add_form_dto)

Add a new form to a specific database.

Add a new form to a specific database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.add_form_dto import AddFormDTO
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
    add_form_dto = client.AddFormDTO() # AddFormDTO |  (optional)

    try:
        # Add a new form to a specific database.
        api_response = await api_instance.add_form_post(database_id, add_form_dto=add_form_dto)
        print("The response of DefaultApi->add_form_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->add_form_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **add_form_dto** | [**AddFormDTO**](AddFormDTO.md)|  | [optional] 

### Return type

**object**

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

# **delete_database_user_delete**
> object delete_database_user_delete(database_id, user_id)

Remove a user's access to a specific database.

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
        # Remove a user's access to a specific database.
        api_response = await api_instance.delete_database_user_delete(database_id, user_id)
        print("The response of DefaultApi->delete_database_user_delete:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->delete_database_user_delete: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **user_id** | **str**|  | 

### Return type

**object**

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

# **get_database_translations_get**
> DatabaseTranslations get_database_translations_get(database_id, language_code)

Fetch all database-level translations for a specific language.

Fetch all database-level translations for a specific language.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.database_translations import DatabaseTranslations
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
        # Fetch all database-level translations for a specific language.
        api_response = await api_instance.get_database_translations_get(database_id, language_code)
        print("The response of DefaultApi->get_database_translations_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_database_translations_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **language_code** | **str**|  | 

### Return type

[**DatabaseTranslations**](DatabaseTranslations.md)

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

# **get_database_tree_get**
> DatabaseTree get_database_tree_get(database_id)

Fetch the full hierarchical tree structure of a database.

Fetch the full hierarchical tree structure of a database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.database_tree import DatabaseTree
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
        # Fetch the full hierarchical tree structure of a database.
        api_response = await api_instance.get_database_tree_get(database_id)
        print("The response of DefaultApi->get_database_tree_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_database_tree_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 

### Return type

[**DatabaseTree**](DatabaseTree.md)

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

# **get_database_users_get**
> object get_database_users_get(database_id)

List all users who have access to the specified database.

List all users who have access to the specified database.

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
        # List all users who have access to the specified database.
        api_response = await api_instance.get_database_users_get(database_id)
        print("The response of DefaultApi->get_database_users_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_database_users_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 

### Return type

**object**

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

# **get_form_get**
> List[object] get_form_get(form_id)

Query and retrieve all records for a specific form.

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
        # Query and retrieve all records for a specific form.
        api_response = await api_instance.get_form_get(form_id)
        print("The response of DefaultApi->get_form_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_form_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **form_id** | **str**|  | 

### Return type

**List[object]**

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

# **get_form_schema_get**
> FormSchema get_form_schema_get(form_id)

Retrieve the design/schema (fields, types, formulas) for a specific form.

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
        # Retrieve the design/schema (fields, types, formulas) for a specific form.
        api_response = await api_instance.get_form_schema_get(form_id)
        print("The response of DefaultApi->get_form_schema_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_form_schema_get: %s\n" % e)
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

# **get_form_translations_get**
> object get_form_translations_get(database_id, form_id, language_code)

Retrieve all translated labels for a specific form and its fields.

Retrieve all translated labels for a specific form and its fields.

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
    form_id = 'form_id_example' # str | 
    language_code = 'language_code_example' # str | 

    try:
        # Retrieve all translated labels for a specific form and its fields.
        api_response = await api_instance.get_form_translations_get(database_id, form_id, language_code)
        print("The response of DefaultApi->get_form_translations_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_form_translations_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **form_id** | **str**|  | 
 **language_code** | **str**|  | 

### Return type

**object**

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

# **get_form_tree_get**
> FormTree get_form_tree_get(form_id)

Fetch the form and all its related forms (references).

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
        # Fetch the form and all its related forms (references).
        api_response = await api_instance.get_form_tree_get(form_id)
        print("The response of DefaultApi->get_form_tree_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_form_tree_get: %s\n" % e)
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

# **get_user_databases_get**
> List[Database] get_user_databases_get()

List all databases the authenticated user has access to.

List all databases the authenticated user has access to.

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

    try:
        # List all databases the authenticated user has access to.
        api_response = await api_instance.get_user_databases_get()
        print("The response of DefaultApi->get_user_databases_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->get_user_databases_get: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**List[Database]**](Database.md)

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

# **query_rows_post**
> List[Dict[str, object]] query_rows_post(query_rows_request)

Query form rows with formulas, filters, and sorting.

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
        # Query form rows with formulas, filters, and sorting.
        api_response = await api_instance.query_rows_post(query_rows_request)
        print("The response of DefaultApi->query_rows_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->query_rows_post: %s\n" % e)
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

# **update_database_post**
> object update_database_post(database_id, update_database_dto=update_database_dto)

Modify database-level settings (e.g., enabled languages).

Modify database-level settings (e.g., enabled languages).

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.update_database_dto import UpdateDatabaseDTO
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
    update_database_dto = client.UpdateDatabaseDTO() # UpdateDatabaseDTO |  (optional)

    try:
        # Modify database-level settings (e.g., enabled languages).
        api_response = await api_instance.update_database_post(database_id, update_database_dto=update_database_dto)
        print("The response of DefaultApi->update_database_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->update_database_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **update_database_dto** | [**UpdateDatabaseDTO**](UpdateDatabaseDTO.md)|  | [optional] 

### Return type

**object**

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

# **update_database_translations_post**
> object update_database_translations_post(database_id, language_code, update_database_translations_dto=update_database_translations_dto)

Update global translations for a specific database.

Update global translations for a specific database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.update_database_translations_dto import UpdateDatabaseTranslationsDTO
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
    update_database_translations_dto = client.UpdateDatabaseTranslationsDTO() # UpdateDatabaseTranslationsDTO |  (optional)

    try:
        # Update global translations for a specific database.
        api_response = await api_instance.update_database_translations_post(database_id, language_code, update_database_translations_dto=update_database_translations_dto)
        print("The response of DefaultApi->update_database_translations_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->update_database_translations_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **language_code** | **str**|  | 
 **update_database_translations_dto** | [**UpdateDatabaseTranslationsDTO**](UpdateDatabaseTranslationsDTO.md)|  | [optional] 

### Return type

**object**

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

# **update_database_user_role_post**
> object update_database_user_role_post(database_id, user_id, update_database_user_role_dto=update_database_user_role_dto)

Modify the assigned role for an existing database user.

Modify the assigned role for an existing database user.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.update_database_user_role_dto import UpdateDatabaseUserRoleDTO
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
    update_database_user_role_dto = client.UpdateDatabaseUserRoleDTO() # UpdateDatabaseUserRoleDTO |  (optional)

    try:
        # Modify the assigned role for an existing database user.
        api_response = await api_instance.update_database_user_role_post(database_id, user_id, update_database_user_role_dto=update_database_user_role_dto)
        print("The response of DefaultApi->update_database_user_role_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->update_database_user_role_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **user_id** | **str**|  | 
 **update_database_user_role_dto** | [**UpdateDatabaseUserRoleDTO**](UpdateDatabaseUserRoleDTO.md)|  | [optional] 

### Return type

**object**

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

# **update_form_records_post**
> update_form_records_post(update_form_records_dto=update_form_records_dto)

Bulk create, update, or delete records across one or more forms.

Bulk create, update, or delete records across one or more forms.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.update_form_records_dto import UpdateFormRecordsDTO
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
    update_form_records_dto = client.UpdateFormRecordsDTO() # UpdateFormRecordsDTO |  (optional)

    try:
        # Bulk create, update, or delete records across one or more forms.
        await api_instance.update_form_records_post(update_form_records_dto=update_form_records_dto)
    except Exception as e:
        print("Exception when calling DefaultApi->update_form_records_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **update_form_records_dto** | [**UpdateFormRecordsDTO**](UpdateFormRecordsDTO.md)|  | [optional] 

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

# **update_form_schema_post**
> object update_form_schema_post(form_id, form_schema=form_schema)

Push a modified schema back to the server to update a form's design.

Push a modified schema back to the server to update a form's design.

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
    form_schema = client.FormSchema() # FormSchema |  (optional)

    try:
        # Push a modified schema back to the server to update a form's design.
        api_response = await api_instance.update_form_schema_post(form_id, form_schema=form_schema)
        print("The response of DefaultApi->update_form_schema_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->update_form_schema_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **form_id** | **str**|  | 
 **form_schema** | [**FormSchema**](FormSchema.md)|  | [optional] 

### Return type

**object**

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

# **update_form_translations_post**
> object update_form_translations_post(database_id, form_id, language_code, update_database_translations_dto=update_database_translations_dto)

Apply new translations to a specific form and its fields.

Apply new translations to a specific form and its fields.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.update_database_translations_dto import UpdateDatabaseTranslationsDTO
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
    update_database_translations_dto = client.UpdateDatabaseTranslationsDTO() # UpdateDatabaseTranslationsDTO |  (optional)

    try:
        # Apply new translations to a specific form and its fields.
        api_response = await api_instance.update_form_translations_post(database_id, form_id, language_code, update_database_translations_dto=update_database_translations_dto)
        print("The response of DefaultApi->update_form_translations_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->update_form_translations_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **form_id** | **str**|  | 
 **language_code** | **str**|  | 
 **update_database_translations_dto** | [**UpdateDatabaseTranslationsDTO**](UpdateDatabaseTranslationsDTO.md)|  | [optional] 

### Return type

**object**

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

# **user_preflight_post**
> UserPreflightResponse user_preflight_post(database_id, user_preflight_dto=user_preflight_dto)

Check the status of a user's email before adding them to a database.

Check the status of a user's email before adding them to a database.

### Example

* Bearer Authentication (bearerAuth):

```python
import client
from client.models.user_preflight_dto import UserPreflightDTO
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
    user_preflight_dto = client.UserPreflightDTO() # UserPreflightDTO |  (optional)

    try:
        # Check the status of a user's email before adding them to a database.
        api_response = await api_instance.user_preflight_post(database_id, user_preflight_dto=user_preflight_dto)
        print("The response of DefaultApi->user_preflight_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->user_preflight_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_id** | **str**|  | 
 **user_preflight_dto** | [**UserPreflightDTO**](UserPreflightDTO.md)|  | [optional] 

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

