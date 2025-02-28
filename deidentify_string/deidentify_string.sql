-- this installation query contains the Python code for the UDF
-- edit to provide your own `vault_id` and `vault_url` values
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

 vault_id = "fb94a86a1e9e4b2e8e161c0dbc8062cc"
 vault_url = "https://ebfc9bee4242.vault.skyflowapis.com"
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
$$