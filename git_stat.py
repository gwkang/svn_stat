import sys
import xml.etree.ElementTree as ElementTree
import json
import argparse
import os
import re
import subprocess
from tqdm import tqdm
import mysql.connector
from datetime import datetime

def insert_to_db(json_data, dict):
    host = json_data['mysql_host']
    user = json_data['mysql_user']
    passwd = json_data['mysql_pass']
    database = json_data['mysql_database']

    mydb = mysql.connector.connect(host=host, user=user, password=passwd)
    cur = mydb.cursor()
    now = datetime.now()
    cur.execute(f'CREATE DATABASE IF NOT EXISTS `{database}`')

    tbl_name = f'GIT_STAT_{now.strftime("%y%m%d_%H%M%S")}'
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{database}`.`{tbl_name}` (
            name VARCHAR(128) NOT NULL PRIMARY KEY, 
            commit_count INT DEFAULT 0, 
            update_files INT DEFAULT 0, 
            update_lines INT DEFAULT 0
        )""")
    query = f'INSERT INTO `{database}`.`{tbl_name}` VALUES '

    #print(dict)
    comma = True
    for key, val in dict.items():
        if key not in json_data['author_white']:
            continue
        if key in json_data['author_ignore']:
            continue

        if comma == False:
            query += ","
        query += f"('{key}', {val['commit_count']}, {val['update_files']}, {val['update_lines']})"
        comma = False
    #print(query)
    cur.execute(query)
    mydb.commit()

def read_commit(f, dict, size):
    # Count all commits
    pbar = tqdm(total=size)

    commit_r = re.compile(r'^commit\s[a-zA-Z0-9]+')
    author_r = re.compile(r'^Author\:\s[^\<]*\<([^@]+).+')
    diff_r = re.compile(r'^diff\s\-\-git.+')
    updated_r = re.compile(r'^\+[^\+]+')

    author = ''
    files = 0
    lines = 0
    
    while True:
        try:
            line = f.readline()
        except UnicodeDecodeError as _:
            continue

        if not line: break

        pbar.update(f.tell() - pbar.n)

        mo = commit_r.search(line)
        if mo != None:
            if lines > 0:
                if author not in dict:
                    dict[author] = { 'commit_count': 0, 'update_files': 0, 'update_lines': 0 }
                
                dict[author]['commit_count'] = dict[author]['commit_count'] + 1
                dict[author]['update_files'] = dict[author]['update_files'] + files
                dict[author]['update_lines'] = dict[author]['update_lines'] + lines

                lines = 0
                files = 0
            continue

        mo = author_r.search(line)
        if mo != None:
            author = mo.group(1)
            #print(f'Author: {author}')
            continue

        mo = diff_r.search(line)
        if mo != None:
            files += 1
            continue

        mo = updated_r.search(line)
        if mo != None:
            lines += 1
            continue
    
    if lines > 0:
        if author not in dict:
            dict[author] = { 'commit_count': 0, 'update_files': 0, 'update_lines': 0 }
        
        dict[author]['commit_count'] = dict[author]['commit_count'] + 1
        dict[author]['update_files'] = dict[author]['update_files'] + files
        dict[author]['update_lines'] = dict[author]['update_lines'] + lines

        lines = 0
        files = 0

    
    pbar.close()

def main():
    parser = argparse.ArgumentParser(description='Get subversion statistics.')
    parser.add_argument('--config', default='git.json', help='JSON formatted config filename.')

    options = parser.parse_args()
    #print(options)

    dict = {}

    #cur_dir = os.getcwd()

    with open(options.config) as json_file:
        json_data = json.load(json_file)

        os.chdir(json_data['path'])
        os.system(f'git log --since="{json_data["start_date"]}" --until="{json_data["end_date"]}" -p > git.log')
        #print(json_data)

        size = os.path.getsize('git.log')

        f = open('git.log', encoding='utf8')
        read_commit(f, dict, size)
        f.close()

        insert_to_db(json_data, dict)


if __name__ == '__main__':
    try:
        main()
    except BrokenPipeError as exc:
        sys.exit(exc.errno)
