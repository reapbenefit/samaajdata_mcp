import os
from os.path import dirname, abspath, exists

if exists("/appdata"):
    data_root_dir = "/appdata"
else:
    root_dir = dirname(abspath(__file__))
    data_root_dir = f"{root_dir}/db"

if not exists(data_root_dir):
    os.makedirs(data_root_dir)


sqlite_db_path = f"{data_root_dir}/db.sqlite"
