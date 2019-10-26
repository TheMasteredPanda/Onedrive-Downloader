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

config = {
    "authority": "https://login.microsoftonline.com/common",
    "scope": ["User.Read", "Files.Read"],
    "client_id": "<hidden>"
}

file_count = 0
folder_count = 0


# app = msal.PublicClientApplication(clientId, authority=config['authority'])
# result = None
# accounts = app.get_accounts()
#
# if accounts:
#     logging.info("Account(s) exists in cache, probably with token too. Let's try.")
#     print("Pick the account you want to use to proceed:")
#     for a in accounts:
#         print(a["username"])
#     # Assuming the end user chose this one
#     chosen = accounts[0]
#     # Now let's try to find a token in cache for this account
#     result = app.acquire_token_silent(config["scope"], account=chosen)
#
# if not result:
#     logging.info("No suitable token exists in cache. Let's get a new one from AAD.")
#
#     flow = app.initiate_device_flow(scopes=config["scope"])
#     if "user_code" not in flow:
#         raise ValueError(
#             "Fail to create device flow. Err: %s" % json.dumps(flow, indent=4))
#
#     print(flow["message"])
#     sys.stdout.flush()  # Some terminal needs this to ensure the message is shown
#
#     # Ideally you should wait here, in order to save some unnecessary polling
#     # input("Press Enter after signing in from another device to proceed, CTRL+C to abort.")
#
#     result = app.acquire_token_by_device_flow(flow)  # By default it will block



def init_db(params):
    if os.path.exists("data.db"):
        return
    else:
        print('Creating data.db')
        connection = sqlite3.connect('data.db')
        c = connection.cursor()

        c.execute('''CREATE TABLE items(
        id TEXT PRIMARY KEY, 
        type INTEGER NOT NULL, 
        name TEXT NOT NULL, 
        size INTEGER NOT NULL, 
        downloaded BOOLEAN DEFAULT FALSE, 
        downloaded_data DATETIME NULL,
        parent_directory TEXT NULL DEFAULT NULL);''')
        print('Created items table.')
        c.execute('''CREATE TABLE urls(
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

        for i in range(math.ceil((len(formatted) / 40))):
            conn = sqlite3.connect('data.db')
            batch = formatted[i * 40: (i + 1) * 40]
            print('Attempting to insert batch sliced from index %s to %s' % (i * 40, (i + 1) * 40))

            cursor = conn.cursor()
            cursor.executemany('INSERT INTO items VALUES (?,?,?,?,?,?,?)', batch)
            conn.commit()
            conn.close()

        # conn2 = sqlite3.connect('data.db')
        # cur1 = conn2.cursor()
        # cur1.executemany("INSERT INTO items VALUES (?,?,?,?,?,?,?);", formatted)
        # conn2.commit()
        # conn2.close()
        # conn3 = sqlite3.connect('data.db')
        # cur = conn3.cursor()
        # cur.executemany("INSERT INTO urls VALUES (?,?)", urls)
        # conn3.commit()
        # conn3.close()

        # print("Inserted %s urls and %s formatted items" % (len(urls), len(formatted)))


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


# def aggregate(params, parent_directory, directory):
#     files = get_files(params, directory)
#
#     for file in files:
#         if 'folder' in file:
#             print("%s/%s is a directory" % (parent_directory, file['name']))
#             # aggregate(params, file['name'])
#         else:
#             print("%s/%s is not a directory" % (parent_directory, file['name']))
#
# # if "access_token" in result
#
# token = "EwB4A8l6BAAUO9chh8cJscQLmU+LSWpbnr0vmwwAAf++mPExp7K41PuQUBBer8CfodzR5mQIi6b/hW4f+1DUvJCJNgPiM751acWk+j67wn3ZOpQftbhzA9Tio4wx4YFib02FHNSRkVmK5Gdc67N8EvotEA71nd7k3rS9zqg4AsZ9Lz0cm9GSEI4gpb3LYl0dqrOHY9uJgOtpnG3jxN40qyh24xJGzfmR2THcTHx6NvaGJT0ligFxJRj/fLdSf2QCmvf47NjytEEDREZPTuQueMq/DhX5RPYUsa8LKHYEYNJ/7jRaC5mn6vHkGbARw+CcNItGCAPLLDMmRV+aTI6Fp3TIGhAQkutX/AJq/BuzNJWMdD6ct+D7cBAp9f5VHzIDZgAACJk1AJ5Ytk8QSAKSyfdX77nmwdKcT2sBkxuacB1IugaeQlY/bChY9DZ+H3HO5brsH6pawLE3GtiV9yWpfyfISjW5ioREtnRAnkxr9+961ZthmMQzSdpXL58oFXPETYMsp22x+5i2rDycyEHXZnQXECTMEBlgLEZy2D34sVSjPplFOTD5bwcdJwDBvydNDM6jNoVqKlG1VY23udEwSHSCtm1d3nD2x6FETZI4sOX5kfmqR/bUm133C/n2zFTbf5Di1kddRg2CSswcgFSfD/EMWKsuTAn5xGdbxa3ZpGJa49Bjm9DwDJqhIGrk8UbgTNjXmcoesvk5GkOVtWctKWSrcgMKN+WCUnK42EjNGZvMsSCr1RD5o6bP/Wcur0BYiIujF1gaSRGwY5wLzq/KeMWEOlUHhUNLvKah5M44gviPHcli7P0Rji1fHhxrrLo5rE/Ds+2e6j7LJx6iqCTasKy5GDGq7ooCKngkbUR0/KG0yqEr+vggGMRyvumF42kIGYqDLzU49YsZbdtycmvZ90YGIUJMt7rhcjeMsj4yMD5ir2HsbSj6Cnt4lP1CPUPP/etIOSQqkOt9740R62XbYMzwQsXobyuFx58Pqewf0erNWV/5nzyiHa5feECFFinEgKtmknhwVNyYSDgUtF+8ZasocO5NBAVirj0wAM1NIN8sL/hjU55ocT7cHL8wlirVmbxhn1+FHyEOFcr50dEQoQzv3qWcgjIrYkz8qNaqn0i6dAibQeA2ai2CEw/wzN7cv4lvnuJOtlvhjEMbsd+x86UdIW9PSYkC"
# params = {"Authorization": "Bearer " + token}
# files = get_files(params, None)
#
# for file in files:
#     if 'folder' in file:
#         print("%s is a directory" % file['name'])
#         aggregate(params, file['name'], file['id'])
#     else:
#         print("%s is not a directory" % file['name'])

# class DownloadWorker(threading.Thread):
#     def __init__(self, queue):
#         threading.Thread.__init__(self)
#         self.queue = queue
#
#     def run(self):
#         while True:
#             try:
#
#             finally:

# def create_folders():
#     folders: {
#
#     }
#
#     for


token_info = get_token(False)
# print(json.dumps(token_info, indent=4))
params = {"Authorization": "%s %s" % (token_info['type'], token_info['token'])}
print(json.dumps(params, indent=4))
init_db(params)



