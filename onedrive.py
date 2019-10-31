import msal
import time
import json
import sys
import requests
import sqlite3
import os
import queue
import threading
import math
import logging
from anytree import Node, RenderTree, AnyNode, PostOrderIter, PreOrderIter
# logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)
from requests import HTTPError

config = {
    "authority": "https://login.microsoftonline.com/common",
    "scope": ["User.Read", "Files.Read"],
    "client_id": "10dffd03-493d-48a9-8aa4-ea62bc66b355"
}

files_iterated = 0
files_folders = 0

def init_db(params):
    if os.path.exists("data.db"):
        return
    else:
        print('Creating data.db')
        connection = sqlite3.connect('data.db')
        connection.execute('''CREATE TABLE items(
        id TEXT PRIMARY KEY,
        type INTEGER NOT NULL,
        name TEXT NOT NULL,
        size INTEGER NOT NULL,
        downloaded BOOLEAN DEFAULT FALSE,
        url TEXT DEFAULT NULL,
        path TEXT NOT NULL,
        parent_directory_id TEXT NULL DEFAULT NULL);''')
        print('Created items table.')
        connection.close()

        nodes = {}
        iter_queue = queue.Queue(maxsize=0)

        def progress():
            print("Files Iterated: %s Files That Are Folders: %s Tasks Queued: %s" % (
                files_iterated, files_folders, iter_queue.qsize()))
            # sys.stdout.flush()

        def iterate_worker():
            while True:
                task = iter_queue.get()

                if task is None:
                    break

                iterate(task)
                progress()
                iter_queue.task_done()

        workers = []

        print('Creating worker threads.')

        for i in range(29):
            t = threading.Thread(target=iterate_worker)
            t.start()
            workers.append(t)
            print('Created worker thread %s' % i)

        def iterate(data):
            # global files_folders, files_iterated
            global files_iterated, files_folders
            files = data['value']

            for file in files:
                files_iterated = files_iterated + 1

                if 'size' in file:
                    size = file['size']
                else:
                    size = -1

                if '@microsoft.graph.downloadUrl' in file:
                    download_url = file['@microsoft.graph.downloadUrl']
                else:
                    download_url = None

                if 'folder' in file:
                    object_type = 2
                else:
                    object_type = 1

                nodes[file['id']] = Node(file['name'], parent=None, entry_id=file['id'],
                                         parent_id=file['parentReference']['id'],
                                         parent_name=file['parentReference']['path'], size=size,
                                         download_url=download_url, type=object_type)
                if 'folder' in file:
                    files_folders = files_folders + 1
                    iter_queue.put(get_entry(params, file['id'], None))

            if '@odata.nextLink' in data:
                iter_queue.put(get_entry(params, None, data['@odata.nextLink']))

        print("Starting iteration of all files and folders in the drive.")
        iterate(get_entry(params, None, None))
        iter_queue.join()

        for i in range(29):
            iter_queue.put(None)

        for t in workers:
            t.join()
            print('Terminating worker thread.')

        print("Indexed %s files and folders" % len(nodes))
        root = Node('onedrive', parent=None, entry_id='1D464A8DD283576C!101')

        for node_key in nodes:
            node = nodes[node_key]

            if node.parent_id in '1D464A8DD283576C!101':
                node.parent = root

            else:
                if node.parent_id not in nodes.keys():
                    print('Parent ID %s/%s is not in keys' % (node.parent_id, node.parent_name))
                    continue
                node.parent = nodes[node.parent_id]

        formatted = []

        for node_key in nodes:
            node = nodes[node_key]
            node_string = str(node)
            node_string = node_string.replace("Node(", "")
            node_string = node_string.replace(")", "")
            split = node_string.split(", ")
            path = split[0].replace("'/", "'")
            formatted.append((
                node.entry_id,
                node.type,
                node.name,
                node.size,
                False,
                node.download_url,
                path,
                node.parent_id
            ))

        print("Formatted %s nodes" % len(formatted))

        conn = sqlite3.connect('data.db')

        iterations = math.ceil(len(formatted) / 200)
        for i in range(iterations):

            if i == iterations:
                sql_batch = formatted[i * 200: -1]
            else:
                sql_batch = formatted[i * 200: (i + 1) * 200]

            conn.executemany("INSERT INTO items VALUES (?,?,?,?,?,?,?,?)", sql_batch)
            conn.commit()
        conn.close()

def get_token(refresh):
    if refresh:
        res = requests.post('https://login.microsoftonline.com/common/oauth2/v2.0/token', params={"client_id": config['client_id'], "grant_type": "refresh_token", "refresh_token": token_info['refresh_token']})

        if res.status_code != 200:
            print("Error: %s" % res.text)
            return
        else:
            res_body = json.loads(res.text)
            return {
                "token": res_body['access_token'],
                "type": res_body['token_type'],
                "refresh_token": res_body['refresh_token'],
                "expires_in": res_body['expires_in'],
                "mili_expires_in": get_current_time_in_miliseconds() + (int(res_body['expires_in']) - 100)
            }

    app = msal.PublicClientApplication(config['client_id'], authority=config['authority'])
    result = None
    accounts = app.get_accounts()

    if accounts:
        print("Account(s) exists in cache, probably with token too. Let's try.")
        print("Pick the account you want to use to proceed:")
        for a in accounts:
            print(a["username"])
        # Assuming the end user chose this one
        chosen = input('Account Username: ')
        chosen_account = None

        for account in accounts:
            if account['username'] is not chosen:
                continue

            chosen_account = account
            break

        # Now let's try to find a token in cache for this account
        result = app.acquire_token_silent(config["scope"], account=chosen_account)

    if not result:
        print("No suitable token exists in cache. Let's get a new one from AAD.")

        flow = app.initiate_device_flow(scopes=config["scope"])
        if "user_code" not in flow:
            raise ValueError(
                "Fail to create device flow. Err: %s" % json.dumps(flow, indent=4))

        print(flow["message"])
        sys.stdout.flush()  # Some terminal needs this to ensure the message is shown

        # Ideally you should wait here, in order to save some unnecessary polling
        # input("Press Enter after signing in from another device to proceed, CTRL+C to abort.")

        result = app.acquire_token_by_device_flow(flow)  # By default it will block

        if "access_token" in result:
            return {
                "token": result['access_token'],
                "type": result['token_type'],
                "refresh_token": result['refresh_token'],
                "expires_in": result['expires_in'],
                "mili_expires_in": get_current_time_in_miliseconds() + (int(result['expires_in']) - 100)
            }

        if "access_token" not in result:
            print('No token returned. An issue occurred.')
            sys.exit(1)

def get_current_time_in_miliseconds():
    return int(time.time()*1000)

def get_entry(params, directory_id, next_link):
    url = 'https://graph.microsoft.com/v1.0/me/drive/root/children'

    if directory_id is not None:
        url = 'https://graph.microsoft.com/v1.0/me/drive/items/' + directory_id + "/children"

    if next_link is not None:
        url = next_link

    response = requests.get(url, headers=params)

    if response.status_code != 200:
        print(response.url)
        print("%s:\n%s" % (response.status_code, response.text))

    json_content = json.loads(response.text)
    return json_content


def build_directory_paths():
    conn = sqlite3.connect('data.db')
    root = Node('onedrive', entry_id=None)
    nodes = {}

    for row in conn.execute("SELECT * FROM items WHERE type=2"):
        # print(row)
        path = row[6]
        path = path[1::]
        path = path[:-1]

        if path.startswith('/'):
            path = path[1::]

        if os.path.exists(path):
            continue

        os.makedirs(path)

def worker():
    while True:
        task = q.get()
        # print(task)

        if task is None:
            break

        if os.path.exists(task['path']):
            if os.path.getsize(task['path']) >= task['size']:
                continue
            else:
                os.os.remove(task['path'])

        try:
            try:
                print('Downloading %s' % task['path'])
                with requests.get(task['url'], stream=True) as r:
                    if r.status_code == 401:
                        break

                    r.raise_for_status()
                    full_path = task['path']

                    if full_path.startswith('/'):
                        full_path = full_path[1::]
                    with open(full_path, 'wb') as f:
                        splitPath = full_path.split('/')
                        splitPath.pop()
                        path = '/'.join(splitPath)

                        if os.path.exists(path) is False:
                            os.makedirs(path)

                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:  # filter out keep-alive new chunks
                                f.write(chunk)
                                # f.flush()
            except HTTPError:
                print('Got http error in attempting to download file named %s' % task['name'])
                q.task_done()
                continue
        finally:
            print("Downloaded file %s - %s Files to Download" % (task['name'], q.qsize()))
            sql_1.put(task['id'])


def sql_worker():
    while True:
        time.sleep(10)
        ids = []

        for id in iter(sql_1.get, None):
            if id is None:
                continue
            ids.append(id)

        connection = sqlite3.connect('data.db')
        connection.executemany('UPDATE items SET downloaded = TRUE WHERE id=?', tuple(ids))
        connection.commit()
        connection.close()
        print('Executed batch of %s updates.' % len(ids))


if os.path.exists('token.json'):
    with open('token.json') as file:
        token_info = json.load(file)
else:
    token_info = get_token(False)
    with open('token.json', 'w') as file:
        json.dump(token_info, file)


params = {"Authorization": "%s %s" % (token_info['type'], token_info['token'])}
print(json.dumps(params, indent=4))
init_db(params)
build_directory_paths()
conn2 = sqlite3.connect('data.db')
q = queue.Queue(maxsize=0)
sql_1 = queue.Queue(maxsize=0)
threads = []
requested = False

for i in range(15):
    t = threading.Thread(target=worker)
    t.start()
    threads.append(t)
    print("Created Downloader Worker %s" % i)

sql_t = threading.Thread(target=sql_worker)
sql_t.start()
threads.append(sql_t)
print('Created SQL Worker')

for item_row in conn2.execute("SELECT * FROM items WHERE downloaded = FALSE AND type = 1"):
    path = item_row[6]
    path = path[1::]
    path = path[:-1]

    q.put({
        'id': item_row[0],
        'type': item_row[1],
        'name': item_row[2],
        'size': item_row[3],
        'downloaded': item_row[4],
        'url': item_row[5],
        'path': path,
        'parent': item_row[7]
    })

print("Tasks Queued: %s" % q.qsize())

q.join()
sql_1.join()

for i in range(15):
    q.put(None)

sql_1.put(None)

for t in threads:
    t.join()
    print('Terminating downloader thread.')

print('Finished tasks')

