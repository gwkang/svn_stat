import sys
import xml.etree.ElementTree as ElementTree
import json
import argparse
import os
import subprocess
from tqdm import tqdm
import mysql.connector
from datetime import datetime

def insert_to_db(host, user, passwd, database, dict):
    mydb = mysql.connector.connect(host=host, user=user, password=passwd, database=database)
    cur = mydb.cursor()
    now = datetime.now()
    cur.execute(f'CREATE DATABASE IF NOT EXISTS `{database}`')

    tbl_name = f'SVN_STAT_{now.strftime("%y%m%d_%H%M%S")}'
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
        if comma == False:
            query += ","
        query += f"('{key}', {val['commit_count']}, {val['update_files']}, {val['update_lines']})"
        comma = False
    #print(query)
    cur.execute(query)
    mydb.commit()


def main():
    parser = argparse.ArgumentParser(description='Get subversion statistics.')
    parser.add_argument('--config', default='config.json', help='JSON formatted config filename.')

    options = parser.parse_args()
    #print(options)

    dict = {}

    #cur_dir = os.getcwd()

    with open(options.config) as json_file:
        json_data = json.load(json_file)

        os.chdir(json_data['path'])
        os.system(f'svn log -v --xml -r {{{json_data["start_date"]}}}:{{{json_data["end_date"]}}} > {json_data["xml_file"]}')
        #print(json_data)
        tree = ElementTree.parse(json_data['xml_file'])
        root = tree.getroot()
        #print(root.tag)
        count = 0
        children = []
        
        for child in root:
            author = child.find('author').text

            if 'author_ignore' in json_data and author in json_data['author_ignore']:
                continue
            
            if 'author_white' in json_data and author not in json_data['author_white']:
                continue
            children.append(child)

        pbar = tqdm(total=len(children))

        for child in children:
            author = child.find('author').text
            revision = int(child.get('revision'))

            # for windows
            cmd = f'svn diff -r {revision-1}:{revision}'
            #print(cmd)
            res = subprocess.check_output(cmd, shell=True)
            plus = 0
            minus = 0

            try:
                dec = res.decode()
            except UnicodeDecodeError as _:
                dec = res.decode('latin-1')

            for line in dec.split("\n"):
                if len(line) < 2:
                    continue
                if line[0] == '+' and line[1] != '+':
                    plus += 1
                elif line[0] == '-' and line[1] != '-':
                    minus += 1

            files = 0
            for _ in child.iter('path'):
                files += 1
            
            if author not in dict:
                dict[author] = { 'commit_count': 0, 'update_files': 0, 'update_lines': 0 }

            dict[author]['commit_count'] = dict[author]['commit_count'] + 1
            dict[author]['update_files'] = dict[author]['update_files'] + files
            dict[author]['update_lines'] = dict[author]['update_lines'] + plus

            #print(dict[author])

            pbar.update(1)
            #print(child.get('revision'))
            #print(child.find('author').text)
            
        pbar.close()

        print(f'Total: {count:d}')

        insert_to_db(json_data['mysql_host'], json_data['mysql_user'], json_data['mysql_pass'], json_data['mysql_database'], dict)


if __name__ == '__main__':
    try:
        main()
    except BrokenPipeError as exc:
        sys.exit(exc.errno)
