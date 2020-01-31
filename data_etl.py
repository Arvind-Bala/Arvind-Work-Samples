import requests
import pandas as pd
import json
import datetime
import pprint
pp = pprint.PrettyPrinter(indent=4)

streak_api_key = '****'
pipeline_url = '****'
call_headers = {'content-type' : 'application/json'}
upswing_pipeline_key = ''****''

upswing_pipeline_response = requests.get(url=(pipeline_url + '/' + upswing_pipeline_key + '/' + 'boxes'), 
                                         auth=(streak_api_key, ''),
                                         headers=call_headers
                                        )
pipeline_fields_response = requests.get(url=(pipeline_url + '/' + upswing_pipeline_key + '/' + 'fields'), 
                                         auth=(streak_api_key, ''),
                                         headers=call_headers
                                        )
pipeline_stage_response = requests.get(url=(pipeline_url + '/' + upswing_pipeline_key + '/' + 'stages'), 
                                         auth=(streak_api_key, ''),
                                         headers=call_headers
                                        )
streak_schools = json.loads(upswing_pipeline_response.text)
pipeline_fields = json.loads(pipeline_fields_response.text)
pipeline_stages = json.loads(pipeline_stage_response.text)
#Streak stuffs all custom columns into a "fields" object. Extract the key_id and name and store it into a new var for later use
pipeline_keys = {key['key'] : key['name'] for key in pipeline_fields}
pipeline_key_names = {pipeline_keys[key] : key for key in pipeline_keys}
#We only care about these objects on each box (i.e. school)
wanted_default_school_fields = ['****','****','****','****','****','****']
#there are a lot of unnecessary custom columns in streak. The wanted_fields list filters for the ones we care about
wanted_custom_field_names = ['****','****']
wanted_custom_field_keys = [pipeline_key_names[name] for name in wanted_custom_field_names]
    
custom_field_value_map = {}
for field in pipeline_fields:
    if 'tagSettings' in list(field.keys()):
        custom_field_value_map[field['name']] = {tag['key'] : tag['tag'] for tag in field['tagSettings']['tags']}
    elif 'dropdownSettings' in list(field.keys()):
            custom_field_value_map[field['name']] = {item['key'] : item['name'] for item in field['dropdownSettings']['items']}

custom_field_value_map['stage_names'] = {key : value['name'] for key, value in pipeline_stages.items()}

filtered_schools = []
for school in streak_schools:
    school_filtered = {}
    for school_field in wanted_default_school_fields:
        #for each school, for each default field on the school, if the object is one of the desired default fields, append to our new filtered variable
        if school_field == 'assignedToSharingEntries' and len(school[school_field]) > 0: 
        #the assignedToSharingEntries variable designates the HEROS, this variable can be empty or an array 
        #with a bunch of additional details about the hero. This is why we check the length of the array to parse accordingly
            school_filtered[school_field] = school[school_field][0]['fullName']
        elif school_field == 'assignedToSharingEntries' and len(school[school_field]) == 0:
            school_filtered[school_field] = ''
        else:
            try:
                if school_field in ['****','****']:
                    school_filtered[school_field] = str(datetime.datetime.fromtimestamp(school[school_field]/1000))
                else:
                    school_filtered[school_field] = school[school_field]
            except:
                continue
    filtered_schools.append(school_filtered)
    
                                   
for school in filtered_schools:
    school['stage'] = custom_field_value_map['stage_names'][school['stageKey']]
    del school['stageKey']
    school_keys = list(school['fields'].keys())
    school_custom_field_keys = list(school['fields'].keys())
    #we turn this into a list because if we did for "field_key in school['fields'].keys()" python 
    #would throw an error because it would update before the loop is done
    for custom_field_key in wanted_custom_field_keys:
        try:
            #Use names instead of keys for easier readability
            custom_field_name = pipeline_keys[custom_field_key]
            #Check if it is a wanted key. If it is wanted, create new record for the key where we use 
            #the name of the column/key rather than the integer lookup value in streak.
            if isinstance(school['fields'][custom_field_key], list):
                if len(school['fields'][custom_field_key]) > 0:
                    school[custom_field_name] = [custom_field_value_map[custom_field_name][item] for item in school['fields'][custom_field_key]]
                else:
                    school[custom_field_name] = ''
            else:
                school[custom_field_name] = (custom_field_value_map[custom_field_name][school['fields'][custom_field_key]] if custom_field_name in list(custom_field_value_map.keys()) else school['fields'][custom_field_key])
            if 'date' in custom_field_name.lower() or 'contact' in custom_field_name.lower():
                school.update({custom_field_name : str(datetime.datetime.fromtimestamp(school[custom_field_name]/1000))})
            #Delete the old record of the key
            del school['fields'][custom_field_key]
        except:
            school[custom_field_name] = ''
    try:
        del school['fields']
    except:
        continue
    
# pp.pprint(filtered_schools)

all_schools_df = pd.DataFrame(filtered_schools)

all_schools_df.fillna(value='', inplace=True)
all_schools_df = all_schools_df[['****']]
all_schools_df.rename(
    columns={'****'},
    inplace=True
)

##########################################################################################
#Initialize variables for Upswing API Call
upswing_api_secret = '****'

url = '****'

headers = {
       'Content-Type': 'application/json',
       '****': upswing_api_secret,
       'Accept': 'application/json'
}

method = '****'

##########################################################################################
#Push data to the DB

errors = {}
for columns, values in all_schools_df.iterrows():
    params = list(values)
    payload = {
      'method': method,
      'params': params,
      'jsonrpc': '2.0',
      'id': 0,
    }
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    print(response.text)
    if 'error' in response.text:
        errors[params[0]] = params
    else:
        continue

pp.pprint(errors)
