import requests
import json
import os, sys
import time
# import PySimpleGUI
import getpass
import logging
import copy
import traceback
from metabase_config import *
import pandas as pd

def create_session(metabase_instance='dev'):
  '''logs on and creates a metabase session
  returns session_id (or response text)
  '''
  global api_url, username
  pw = getpass.getpass('Please type in the password for '+metabase_instance)
  #pw = input()
  payload = json.dumps({"username": username[metabase_instance], "password": pw})
  resp = requests.post(api_url[metabase_instance]+'/session',
                       headers={'Content-Type': 'application/json'},
                       data=payload)
  return resp.text

def create_collection(coll_name, parent_coll_id, session, metabase_instance='dev'):
  
  global collection_payload, api_url, collection_ids
  s = json.loads(session)
  #print(s)
  headers = {'Content-Type': 'application/json', 'X-Metabase-Session': s['id']}
  payload = copy.deepcopy(collection_payload)
  payload["parent_id"] = parent_coll_id
  payload["name"] = coll_name
  payload=json.dumps(payload)
  #print(parent_coll_id, payload)
  resp = requests.post(api_url[metabase_instance]+'/collection',
                       headers=headers,
                       data=payload)
  #print(resp.text)
  response = json.loads(resp.text)
  collection_ids[metabase_instance][coll_name] = response["id"]
  #print(collection_ids)

#session = create_session()
#create_collection('tmp1', 99, session)
#create_collection('tmp3', 99, session)

def populate_collections(metabase_instance, root_dir='.'):
  '''creates appropriate collections after traversing directories
  '''
  global collection_ids #, card_payload, collection_payload
  # first create a session on the relevant metabase instance
  session = create_session(metabase_instance)  
  walker = os.walk(root_dir)
  for x in walker:
    # first add all .sql files to the current collection
    for z in x[2]:
      if z.endswith('.sql') and x[0]!='.':
        #print('adding', z, 'to collection', x[0], '('+str(collection_ids[metabase_instance][x[0]])+')')
        full_path = os.path.join(x[0],z)
        collection = collection_ids[metabase_instance][x[0]]
        add_query(full_path, collection, metabase_instance, session)
    # then create subcollections
    for y in x[1]:
      coll_name = y
      parent_coll_id = collection_ids[metabase_instance][x[0]]
      create_collection(coll_name, parent_coll_id, session, metabase_instance=metabase_instance)
      #j+=1
      j = collection_ids[metabase_instance][coll_name]
      print('created collection', y, '('+str(j)+')', 'in', x[0], '('+str(collection_ids[metabase_instance][x[0]])+')')
      collection_ids[metabase_instance][os.path.join(x[0],y)]=j
  return collection_ids

def add_query(file, collection, metabase_instance, session):
  
  global card_payload
  s = json.loads(session)
  print(s)
  headers = {'Content-Type': 'application/json', 'X-Metabase-Session': s['id']}
  print('adding', file, 'to', collection)
  fields_to_change = ['collection_id', 'dataset_query', 'native', 'description', 'name']
  #for file in sorted(os.listdir('.')):
  if file.endswith('.sql'):
    with open(file, 'r') as fobj:
      first_line = fobj.readline().strip()
      if first_line.startswith('--'):
        description = first_line.replace('-- ', '')
      else:
        description = None
      name = file.replace('.sql', '').replace('.', '').replace('/',' ').strip()
      query = first_line+'\n'+fobj.read()
    # print('+++\n'+name)
    # print(description)
    # print('++')
    # print(query)
    card_payload["collection_id"] = collection
    card_payload["name"] = name
    card_payload["description"] = description
    card_payload["dataset_query"]["native"]["query"] = query
    card_payload["native"]["query"] = query
    card_payload["query"] = query
    card_payload["template-tags"] = {}
    #print(json.dumps(basic_payload))
    #try:
    payload = json.dumps(card_payload)
    try:
      print('trying', file, '...')
      r = requests.post(api_url[metabase_instance]+'/card', 
        headers = headers, 
        data = payload,
        timeout = 10)
      #s = json.loads(r.text)
      print('succeeded!') #, "here's the response:", r.text)
      print('now sleeping...')
      time.sleep(3)
      print('done!')
    except:
      print(file, 'failed!')
      print('retrying after commenting out the query...')
      query = 'select 1 /* ' + query + ' */'
      card_payload["dataset_query"]["native"]["query"] = query
      card_payload["native"]["query"] = query
      card_payload["query"] = query
      payload = json.dumps(card_payload)
      try:
        r = requests.post(api_url[metabase_instance]+'/card', 
          headers = headers, 
          data = payload,
          timeout = 10)
        #s = json.loads(r.text)
        print('\tsucceeded!') #, "here's the response:", r.text)
        print('\tnow sleeping...')
        time.sleep(3)
        print('\tdone!')
        print('succeeded with commented out query.')
      except:
        print(file, 'failed again with commented out query.')
        session = create_session(metabase_instance)
        print(session)
    # sys.exit(0)

def fetch_all_cards(metabase_instance):
  ''' fetches all cards from a metabase instance
  '''
  # first create a session
  try:
    session = create_session(metabase_instance)
    s = json.loads(session)
    headers = {'Content-Type': 'application/json', 'X-Metabase-Session': s['id']}
  except Exception as err:
    print("couldn't create session.")
    traceback.print_tb(err.__traceback__)
    sys.exit()
  # then fetch the cards
  try:
    r = requests.get(api_url[metabase_instance]+'/card', 
                     headers = headers) 
    # print(r.text)
    print('fetched everything from', metabase_instance, '.')
    return r.text
  except Exception as err:
    print("couldn't fetch cards.")
    traceback.print_tb(err.__traceback__)
    sys.exit()


def fetch_all_collections(metabase_instance):
  ''' fetches all collections from a metabase instance
  '''
  # first create a session
  try:
    session = create_session(metabase_instance)
    s = json.loads(session)
    headers = {'Content-Type': 'application/json', 'X-Metabase-Session': s['id']}
  except Exception as err:
    print("couldn't create session.")
    traceback.print_tb(err.__traceback__)
    sys.exit()
  # then fetch the cards
  try:
    r = requests.get(api_url[metabase_instance]+'/collection', 
                     headers = headers) 
    # print(r.text)
    print('fetched everything from', metabase_instance, '.')
    return r.text
  except Exception as err:
    print("couldn't fetch collections.")
    traceback.print_tb(err.__traceback__)
    sys.exit()

def fetch_all_dashboards(metabase_instance):
  # first create a session
  try:
    session = create_session(metabase_instance)
    s = json.loads(session)
    headers = {'Content-Type': 'application/json', 'X-Metabase-Session': s['id']}
  except Exception as err:
    print("couldn't create session.")
    traceback.print_tb(err.__traceback__)
    sys.exit()
  # then fetch the cards
  try:
    r = requests.get(api_url[metabase_instance]+'/dashboard', 
                     headers = headers) 
    # print(r.text)
    print('fetched everything from', metabase_instance, '.')
    return r.text
  except Exception as err:
    print("couldn't fetch dashboards.")
    traceback.print_tb(err.__traceback__)
    sys.exit()

def fetch_all_pulses(metabase_instance):
  # first create a session
  try:
    session = create_session(metabase_instance)
    s = json.loads(session)
    headers = {'Content-Type': 'application/json', 'X-Metabase-Session': s['id']}
  except Exception as err:
    print("couldn't create session.")
    traceback.print_tb(err.__traceback__)
    sys.exit()
  # then fetch the cards
  try:
    r = requests.get(api_url[metabase_instance]+'/pulse', 
                     headers = headers) 
    # print(r.text)
    print('fetched everything from', metabase_instance, '.')
    return r.text
  except Exception as err:
    print("couldn't fetch pulses.")
    traceback.print_tb(err.__traceback__)
    sys.exit()

def fetch_all_alerts(metabase_instance):
  # first create a session
  try:
    session = create_session(metabase_instance)
    s = json.loads(session)
    headers = {'Content-Type': 'application/json', 'X-Metabase-Session': s['id']}
  except Exception as err:
    print("couldn't create session.")
    traceback.print_tb(err.__traceback__)
    sys.exit()
  # then fetch the cards
  try:
    r = requests.get(api_url[metabase_instance]+'/alert', 
                     headers = headers) 
    # print(r.text)
    print('fetched everything from', metabase_instance, '.')
    return r.text
  except Exception as err:
    print("couldn't fetch alerts.")
    traceback.print_tb(err.__traceback__)
    sys.exit()


if __name__=='__main__':
  if len(sys.argv) == 2 and sys.argv[1] in ['dev', 'prod']:
    print("let's not post cards again, shall we?")
    # populate_collections(sys.argv[1])
  elif len(sys.argv) == 3 and sys.argv[2] == 'get':
    # get cards
    json_text = fetch_all_cards(sys.argv[1])
    with open(sys.argv[1]+'_cards.json', 'w') as f:
      f.write(json_text)
    try:
      cards_df = pd.read_json(json_text)
      # print(cards_df.columns)
      # print(cards_df.head())
      cards_df.to_csv(sys.argv[1]+'_cards.csv', index=False)
    except:
      print("couldn't create and write dataframe.")
    # get collections
    json_text = fetch_all_collections(sys.argv[1])
    with open(sys.argv[1]+'_collections.json', 'w') as f:
      f.write(json_text)
    try:
      collections_df = pd.read_json(json_text)
      # print(json_text)
      # for i in json.loads(json_text):
      #   print('--\n', i)
      # print(cards_df[cards_df['collection_id'].isna()]['id'].tolist())
      # print(collections_df[collections_df['id'].isna()])
      # sys.exit()
      cards_df['collection_id'] = cards_df['collection_id'].astype(str)
      collections_df['id'] = collections_df['id'].astype(str)
      cards_df['collection_id'].fillna(value='root')
      full_df = pd.merge(cards_df, collections_df, how='left', 
                        left_on='collection_id', right_on='id', 
                        suffixes=('_card', '_coll'))
      full_df.to_csv(sys.argv[1]+'_cards_w_colls.csv', index=False)
      collections_df.to_csv(sys.argv[1]+'_collections.csv', index=False)
    except:
      pass
      print("couldn't create and write dataframes.")
    # get dashboards
    json_text = fetch_all_dashboards(sys.argv[1])
    with open(sys.argv[1]+'_dashboards.json', 'w') as f:
      f.write(json_text)
    # get pulses
    json_text = fetch_all_pulses(sys.argv[1])
    with open(sys.argv[1]+'_pulses.json', 'w') as f:
      f.write(json_text)
    json_text = fetch_all_alerts(sys.argv[1])
    with open(sys.argv[1]+'_alerts.json', 'w') as f:
      f.write(json_text)


  else:
    print('need some arguments.')
