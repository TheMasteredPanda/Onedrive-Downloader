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
from anytree import Node, RenderTree, AnyNode, PostOrderIter
from anytree.dotexport import RenderTreeGraph

config = {
    "authority": "https://login.microsoftonline.com/common",
    "scope": ["User.Read", "Files.Read"],
    "client_id": "<hidden>"
}

file_count = 0
folder_count = 0


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
        downloaded_data DATETIME NULL,
        parent_directory TEXT NULL DEFAULT NULL);''')
        print('Created items table.')
        connection.execute('''CREATE TABLE urls(
        id TEXT PRIMARY KEY,
        url TEXT NOT NULL)''')
        connection.close()
        formatted = []
        urls = []

        def walk_files(directory_id):
            files = None

            if not directory_id:
                files = get_files(params, None)
            else:
                files = get_files(params, directory_id)

            for file in files:
                global file_count
                global folder_count

                type = None

                if "folder" in file:
                    type = 2
                    folder_count = folder_count + 1
                    walk_files(file['id'])
                else:
                    if "file" not in file:
                        continue
                    type = 1
                    urls.append((file['id'], file['@microsoft.graph.downloadUrl']))
                    file_count = file_count + 1

                sys.stdout.write("\rFiles Formatted: %d Folder Formatted: %d" % (file_count, folder_count))
                sys.stdout.flush()
                formatted.append((file['id'], type, file['name'], file['size'], False, None, directory_id))

        walk_files(None)
        print('\nFormatted %s File(s)/Folder(s)' % len(formatted))
        print("Inserting formatted entries into database.")
        object_array_amount = math.ceil((len(formatted) / 100))
        conn = sqlite3.connect('data.db')
        for i in range(object_array_amount):

            if i == object_array_amount:
                batch = formatted[i * 100: -1]
            else:
                batch = formatted[i * 100: (i + 1) * 100]
            print('Attempting to insert batch sliced from index %s to %s' % (i * 100, (i + 1) * 100))

            # cursor = conn.cursor()
            conn.executemany('INSERT INTO items VALUES (?,?,?,?,?,?,?)', batch)
            conn.commit()

        urls_array_amount = math.ceil((len(urls) / 100))

        for i in range(urls_array_amount):
            if i == urls_array_amount:
                batch = urls[i * 100: -1]
            else:
                batch = urls[i * 100: (i + 1) * 100]

            print(batch)
            print('Attempting to insert url batch sliced from index %s to %s' % (i * 100, (i + 1) * 100))
            conn.executemany('INSERT INTO urls VALUES (?,?)', batch)
            conn.commit()
        conn.close()
        print("Created database and populated tables.")


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


def get_files(params, directory_id):
    url = 'https://graph.microsoft.com/v1.0/me/drive/root/children'

    if directory_id:
        url = 'https://graph.microsoft.com/v1.0/me/drive/items/' + directory_id + "/children"

    response = requests.get(url, headers=params)

    if response.status_code != 200:
        print(response.url)
        print("%s:\n%s" % (response.status_code, response.text))

    # print(response.url)
    return json.loads(response.text)['value']


def build_directory_paths():
    conn = sqlite3.connect('data.db')
    root = Node('onedrive', entry_id=None)
    nodes = {}
    unformatted_paths = {}

    for row in conn.execute("SELECT * FROM items WHERE type=2"):
        unformatted_paths[row[0]] = {
            "parent": row[6],
            "name": row[2],
            "id": row[0],
            "unformatted": True
        }

    for key in unformatted_paths:
        entry = unformatted_paths[key]
        nodes[key] = Node(entry['name'], parent=None, parent_id=entry['parent'], entry_id=entry['id'])

    for node_key in nodes:
        node = nodes[node_key]

        if node.parent_id is None:
            node.parent = root
        else:
            node.parent = nodes[node.parent_id]

    def make(root, path):
        if len(root.children) == 0:
            return

        for child in root.children:
            if os.path.exists('%s/%s' % (path, child.name)):
                continue

            os.makedirs("%s/%s" % (path, child.name))
            make(child, "%s/%s" % (path, child.name))

    make(root, "onedrive")
    conn.close()
    print("Directories made.")
    return root

def worker():
    while True:
        task = q.get()

        if task is None:
            break

        full_path = "%s/%s" % (task['path'], task['name'])

        if os.path.exists(full_path):
            if os.path.getsize(full_path) >= task['size']:
                continue
            else:
                os.os.remove(full_path)

        file = requests.get(task['url'])

        try:
            print('Downloading %s', task['name'])
            open(full_path, 'wb').write(file.content)
        finally:
            print("Downloaded file %s" % task['name'])
            conn = sqlite3.connect('data.db')
            conn.execute("UPDATE items SET downloaded = TRUE WHERE id = ?", (task['id'],))
            conn.commit()
            q.task_done()


token_info = get_token(False)
# print(json.dumps(token_info,
# indent=4))
params = {"Authorization": "%s %s" % (token_info['type'], token_info['token'])}
print(json.dumps(params, indent=4))
init_db(params)
root_node = build_directory_paths()
conn2 = sqlite3.connect('data.db')
q = queue.Queue(maxsize=0)
threads = []

for i in range(2):
    t = threading.Thread(target=worker)
    t.start()
    threads.append(t)
    print("Created Worker %s" % i)

for item_row in conn2.execute("SELECT * FROM items WHERE downloaded = false AND type = 1"):
    cursor = conn2.cursor()
    cursor.execute("SELECT * FROM urls WHERE id = ?", (item_row[0],))
    result = cursor.fetchone()
    if result is None:
        continue
    else:
        # print([node.path for node in PostOrderIter(root_node, filter_=lambda e: e.entry_id == item_row[6])][0])
        path = [node for node in PostOrderIter(root_node, filter_=lambda e: e.entry_id == item_row[6])][0]
        # path = 'onedrive'
        string_path = ''

        for entry in path.path:
            if entry.depth <= path.depth:
                if entry.depth == 0:
                    string_path = entry.name
                else:
                    string_path = string_path + '/' + entry.name
                continue

            break

        url = result[1]
        id = result[0]
        size = item_row[3]
        name = item_row[2]

        q.put({
            'path': string_path,
            'url': url,
            'id': id,
            'size': size,
            'name': name
        })


print("Tasks Queued: %s" % (q.qsize()))

q.join()





