# Deidentify String Function for Databricks

This function takes any string, detects sensitive enitities in the string, and replaces sensitive data with non-sensitive tokens.

```
results = deidentify_string(string, access_token)
```

> [!NOTE]
> This function is in active development and is meant to serve as an example. Test and validate before deploying.

## Installation

To install this function in your Databricks account, copy the contents of [deidentify_string.sql](/deidentify_string/deidentify_string.sql) into a Query in Databricks, replace the `vault_id` and `vault_url` variables, and run it by passing in a `string` and a `sky_api_key`.

1. Copy the contents
2. Get details from Skyflow
3. Replace values in the code
4. Execute the query to install the function
5. Test the function

### Prerequisite: configure secrets

To use this function as written you must configure a Secret in Databricks for storing the Skyflow API credentials.

#### Install the CLI

If you use Homebrew on a Mac, the below commands will complete the install.

```
brew tap databricks/tap
brew install databricks
```

For more detailed instructions for any dev environment see the official documentation: [Databricks | Install or update the Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install)

##### Configure the CLI

To get started and create a configuration profile on your machine, run `databricks configure`.

You should be prompted for `Databricks Host` and a `Personal Access Token`. 

To get a Personal Access Token (PAT) for development login to the Databricks UI, open Settings, click Developer, then Access Tokens.

![create an access token in Databricks](/assets/access_tokens_ui.png)

For more information on authenticating the Databricks CLI see the official documentation: [Databricks | Authentication for the Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/authentication)

#### Configure a secret scope in Databricks

Now that you've configured and authenticated the Databricks CLI, run the following command to create a 'scope' for your secrets in Databricks: 

`databricks secrets create-scope <scope-name>`

For the rest of this demo we'll use the scope `sky-agentic-demo`.

`databricks secrets create-scope sky-agentic-demo`

#### Get details from Skyflow

- Create or log into your account at [skyflow.com](https://skyflow.com) and generate an API key: [docs.skyflow.com](https://docs.skyflow.com/api-authentication/)
- Copy your API key, Vault URL, and Vault ID


#### Store the secrets in Databricks

Create your secrets using the JSON syntax:

```sh
databricks secrets put-secret --json '{
  "scope": "sky-agentic-demo",
  "key": "sky_api_key",
  "string_value": "--sky_api_key--"
}'
```

To confirm the secrets have been uploaded successfully, run `databricks secrets list-secrets sky-agentic-demo` to see a list of the keys you provided and an updated timestamp.

Example:

```sh
Key            Last Updated Timestamp
sky_api_key    1739998630197
sky_vault_id   1739998644986
sky_vault_url  1739998639685
```

Then to read a secret in a Notebook, use `dbutils.secrets`:

`sky_api_key = dbutils.secrets.get(scope = "sky-agentic-demo", key = "sky_api_key")`

To learn more about Secrets in Databricks, see the official documentation: [Secret Management | Databricks](https://docs.databricks.com/aws/en/security/secrets)

### Create a simple de-identification tool as a Function

An agent "tool" is a function the agent can choose to invoke to perform some action. In this example, we'll create a simple tool which will enable the agent to deidentify a string and remove all possibly-sensitive data from the unstructured text.

#### Function definition in Python

First let's examine the Python code for the Python function itself. To use this in your own account, complete the TODOs in the sample code below:

```py
import sys
import json
import requests
from io import StringIO

sys_stdout = sys.stdout
redirected_output = StringIO()
sys.stdout = redirected_output

if sky_api_key is None or sky_api_key == '':
   # try to fetch the API key from env variables
   bearer_token = os.environ.get("SKY_API_KEY")
else:
   bearer_token = sky_api_key
# set the Vault ID
vault_id = "--vault_id--"
# set the Vault URL
vault_url = "--vault_url--"
api_path = "/v1/detect/deidentify/string"
api_url = vault_url + api_path
headers = {
   "Authorization": f"Bearer {bearer_token}",
   "Content-Type": "application/json",
}
json_body = {
   "vault_id": vault_id,
   "text": input_text,

}
# "restrict_entities": ["NAME"]

try:
   api_response = requests.post(api_url, headers=headers, json=json_body)
   api_response.raise_for_status()
   external_data = api_response.json()
   result = external_data.get('processed_text', 'No processed_text found')
except requests.exceptions.RequestException as e:
   result = f"Error calling external API: {str(e)}"

sys.stdout = sys_stdout
return result
```

#### Install the tool as a function in Databricks Unity Catalog

To create this function in Databricks Unity Catalog, click 'Create a Query' and run the query below. 

```sql
CREATE OR REPLACE FUNCTION
agentic.default.deidentify_string (
 input_text STRING COMMENT 'The string to be de-identified.',
 sky_api_key STRING COMMENT 'The API key for the Skyflow API.'
)
RETURNS STRING
LANGUAGE PYTHON
DETERMINISTIC
COMMENT 'Deidentify a string using the Skyflow API. Removes any sensitive data from the string and returns a safe string with placeholders in place of sensitive data tokens.'
AS $$
  -- Python function
$$
```

**See [deidentify_string.sql](/deidentify_string/deidentify_string.sql) for the full query with function code.**

1. Be sure to complete the TODOs in the Python code to make this functional. 
2. Specify the name of the catalog you want to use in place of "your_catalog". You will need write permissions to the catalog, 
3. Copy and paste your Python code where it says "# python code from the previous step".
4. Run the query to install the function.
5. Try it out (see Usage below).

## Usage

From a Query (SQL):

```py
results = deidentify_string(string, access_token)
```

From a Notebook (Python + Spark):

```py
example
```

As an Agent tool:

```py

```