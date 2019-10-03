import requests
import base64
import json
import singer
import os
import csv
import codecs
import xlrd
from singer import metadata

schemas = {}
SDC_EXTRA_COLUMN = "_sdc_extra"

KEY_PROPERTIES = {
    'ap_workspace': ['guid'],
    'ap_models': ['id']
}

REQUIRED_CONFIG_KEYS = ['username', 'service_url','workspace','models','filenames']
LOGGER = singer.get_logger()


def header_payload(p_data):

    username = p_data['username']
    cert = open('cert_anaplan.pem').read()
    user = 'AnaplanCertificate ' + str(base64.b64encode((f'{username}:{cert}').encode('utf-8')).decode('utf-8'))
    header = {
        'Authorization': user
    }
    return header


def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', KEY_PROPERTIES[schema_name])
    for field_name in schema['properties'].keys():
        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return mdata


def get_catalog():
    raw_schemas = load_schemas()
    streams = []
    for schema_name, schema in raw_schemas.items():
        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)
        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': metadata.to_list(mdata),
            'key_properties': KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)
        streams.reverse();
    return {'streams': streams}


def do_discover():
    LOGGER.info('Loading schemas')
    catalog = get_catalog()
    LOGGER.info (json.dumps(catalog, indent=2))


def do_sync(p_data,state,p_catalog):
    header = header_payload(p_data)
    for stream in p_catalog['streams']:
        stream_id = stream['tap_stream_id']
        stream_schema = stream['schema']
        mdata = stream['metadata']
        if stream_id == 'ap_workspace':
            url = p_data['service_url'] + "workspaces"
            load_workspace(p_data, stream_id, url, header,state)
        if stream_id == 'ap_models':
            if workspace_list:
                for lst_ws in workspace_list:
                    url = p_data['service_url'] + "workspaces/" + lst_ws + "/models"
                    load_models(p_data,stream_id,url,header,state)
                    if model_list:
                        for lst_model in model_list:
                            url = p_data['service_url'] + "workspaces/" + lst_ws + "/models/" + lst_model +"/exports"
                            export_definition(url,header)
                            export_list.sort()
                            if export_list:
                                filename_list=input_list(p_data,'filenames')
                                filename_list.sort()
                                if filename_list:
                                    for lst_exp in export_list:
                                        for lst_file in filename_list:
                                            if str(lst_exp).strip() == str(lst_file).strip():
                                                tmp_str=str(lst_exp).replace("-","")
                                                ext_str=os.path.splitext(tmp_str)[0]
                                                ext_str = " ".join(ext_str.split())
                                                file_name='ap_'+ ext_str.replace(" ","_")
                                                extension = os.path.splitext(lst_exp)[1]
                                                if extension == ".xls":
                                                    file_url = p_data['service_url'] + "workspaces/" + lst_ws + "/models/" + lst_model + "/exports/" + lst_exp +"/tasks"
                                                    task=export_task(file_url,header)
                                                    if task:
                                                        file_url = p_data['service_url'] + "workspaces/" + lst_ws + "/models/" + lst_model + "/files/" + lst_exp
                                                        excel_data = write_excel_file(file_url,header,lst_exp)
                                                        if excel_data:
                                                            singer.write_schema(file_name, {}, [])
                                                            for xls_data in excel_data:
                                                                singer.write_record(file_name,xls_data)
                                                                singer.write_state(state)
                                                            delete_file(lst_exp)
                                                else:
                                                    file_url = p_data[ 'service_url'] + "workspaces/" + lst_ws + "/models/" + lst_model + "/files/" + lst_exp
                                                    load_file_details(file_url, header, file_name,state)

                                else:
                                    for lst_exp in export_list:
                                        lst_exp=str(lst_exp).strip()
                                        tmp_str = str(lst_exp).replace("-", "")
                                        ext_str = os.path.splitext(tmp_str)[0]
                                        ext_str = " ".join(ext_str.split())
                                        file_name = 'ap_' + ext_str.replace(" ", "_")
                                        extension = os.path.splitext(lst_exp)[1]
                                        if extension == ".xls":
                                            file_url = p_data['service_url'] + "workspaces/" + lst_ws + "/models/" + lst_model + "/exports/" + lst_exp + "/tasks"
                                            task = export_task(file_url, header)
                                            if task:
                                                file_url = p_data['service_url'] + "workspaces/" + lst_ws + "/models/" + lst_model + "/files/" + lst_exp
                                                excel_data = write_excel_file(file_url, header, lst_exp)
                                                if excel_data:
                                                    singer.write_schema(file_name, {}, [])
                                                    for xls_data in excel_data:
                                                        singer.write_record(file_name, xls_data)
                                                        singer.write_state(state)
                                                    delete_file(lst_exp)
                                        else:
                                            file_url = p_data['service_url'] + "workspaces/" + lst_ws + "/models/" + lst_model + "/files/" + lst_exp
                                            load_file_details(file_url, header, file_name,state)


            else:
                exit(0)




def delete_file(p_file_name):
    if os.path.exists(p_file_name):
       os.remove(p_file_name)


def export_task (p_url,p_header):
    response=requests.post(p_url,json={"localeName": "en_US"},headers=p_header)
    if (response.status_code == 200):
        end = None
        data = response.text
        start_index= data.index(':') +2
        task_id=data[start_index: end]
        task_id=task_id[1:-3]
    return task_id


def write_excel_file(p_url,p_header,p_filename):
    response = requests.request("GET", p_url, headers=p_header)
    if (response.status_code == 200):
        file = open(p_filename, 'wb')
        file.write(response.content)
        file.close()
        workbook = xlrd.open_workbook(p_filename, on_demand=True)
        worksheet = workbook.sheet_by_index(0)
        first_row = []  # The row where we stock the name of the column
        for col in range(worksheet.ncols):
            first_row.append(worksheet.cell_value(0, col))
        # transform the workbook to a list of dictionaries
        data = []
        for row in range(1, worksheet.nrows):
            elm = {}
            for col in range(worksheet.ncols):
                elm[first_row[col]] = worksheet.cell_value(row, col)
            data.append(elm)
        workbook.release_resources()
        return data



def load_workspace(p_data, p_schema, p_url, p_header,p_stae):
    global workspace_list
    workspace_list=[]
    singer.write_schema(p_schema, schemas[p_schema], 'guid')
    response = requests.request("GET", p_url, headers=p_header)
    data = response.json()
    if (response.status_code == 200):
        for record in data:
            if (record['name'] == p_data['workspace']):
                workspace_list.append(record['guid'])
                singer.write_record(p_schema, record)
                singer.write_state(p_stae)
            if (p_data['workspace'] == ""):
                workspace_list.append(record['guid'])
                singer.write_record(p_schema, record)
                singer.write_state(p_stae)
    else:
        exit(1)



def load_models(p_data,p_schema,p_url,p_header,p_stae):
    global model_list
    model_list = []
    ip_model_list=input_list(p_data,'models')
    if ip_model_list:
        singer.write_schema(p_schema, schemas[p_schema], 'id')
        response = requests.request("GET", p_url, headers=p_header)
        data = response.json()
        if (response.status_code == 200):
            for record in data:
                for ip_models in ip_model_list:
                    if (str(ip_models).strip() == record['name']):
                        model_list.append(record['id'])
                        singer.write_record(p_schema, record)
                        singer.write_state(p_stae)
        else:
            exit(1)
    else:
        singer.write_schema(p_schema, schemas[p_schema], 'id')
        response = requests.request("GET", p_url, headers=p_header)
        data = response.json()
        if (response.status_code == 200):
            for record in data:
                singer.write_record(p_schema, record)
                singer.write_state(p_stae)
        else:
            exit(1)


def export_definition(p_url,p_header):
    global export_list
    export_list = []
    response = requests.request("GET", p_url, headers=p_header)
    data = response.json()
    if (response.status_code == 200):
        for record in data:
            export_list.append(record['name'])
    else:
        exit(1)
    export_list


def input_list (p_data,p_type):
    ip_list = []
    str_input=""
    if p_type == 'filenames':
        str_input = str(p_data['filenames']).strip('[]')
    if p_type == 'models':
       str_input = str(p_data['models']).strip('[]')
    if str_input:
        for lst_input in str_input.split(','):
            lst_input=lst_input.replace("'","")
            ip_list.append(lst_input)
    return ip_list



def get_row_iterator(iterable, options=None):
    options = options or {}
    file_stream = codecs.iterdecode(iterable, encoding='ISO-8859-1')
    field_names = None
    # Replace any NULL bytes in the line given to the DictReader
    reader = csv.DictReader((line.replace('\0', '') for line in file_stream), fieldnames=field_names, restkey=SDC_EXTRA_COLUMN, delimiter=options.get('delimiter', ','))
    headers = set(reader.fieldnames)
    if options.get('key_properties'):
        key_properties = set(options['key_properties'])
        if not key_properties.issubset(headers):
            raise Exception('CSV file missing required headers: {}, file only contains headers for fields: {}'
                            .format(key_properties - headers, headers))
    if options.get('date_overrides'):
        date_overrides = set(options['date_overrides'])
        if not date_overrides.issubset(headers):
            raise Exception('CSV file missing date_overrides headers: {}, file only contains headers for fields: {}'
                            .format(date_overrides - headers, headers))
    return reader


def load_file_details(p_url, p_header, p_filename,p_state):
    response = requests.request("GET", p_url, headers=p_header)
    if (response.status_code == 200):
        singer.write_schema(p_filename, {}, [])
        row_iterator = get_row_iterator(response.iter_lines())
        for record in row_iterator:
            singer.write_record(p_filename, record)
            singer.write_state(p_state)
    else:
        exit(1)



def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    global schemas
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.config:
        if args.discover:
            do_discover()
        else:
            catalog = args.properties if args.properties else get_catalog()
            do_sync(args.config, args.state, catalog)


if __name__ == '__main__':
    main()
