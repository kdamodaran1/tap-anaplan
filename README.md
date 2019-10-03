
This tap:
  - Pulls raw data from the Anaplan Api
  - Extracts the following resources: 
      - Workspace
      - Models
      - Report file (.csv and .xls files)
      
  - Outputs the schema for each resource
  - Full table load 
  
## Requirements and Installation
For more requirements,example and information about running a singer tap see the 
[singer instructions](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md)

##  Anaplan Certificate 
The Anaplan api are access through the Anaplan certificate Authorization, so get the .pem certificate file from the anaplan administrator and placed it under the virtual environment

## Usage
Source config file 
  - This config contains user name,service url,workspace name,models name,file names 
  
        {
            "api_key": "0XXXXXX",         
                         
            "service_url": ""
        }
 
## Run the Tap
    tap-okta.py -c config.json | target-stitch -c target_config.json
  
 Messages are written to standard output following the Singer specification. The resultant stream of JSON data can be consumed by a Singer target.
    
### Pagination:
 By Default 200 records are extracted from  the source json payload, so the pagination logic is implemented to loop through all the records from source json payload and load into the stitch target 
  
## Replication Methods and State File
  - Full Table
       - groups
       - users
       - applications
  - State File
       - None.
