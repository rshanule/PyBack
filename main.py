import sqlite3
import uuid
import os
import shutil
import hashlib
from sqlite3 import Connection, Error

backupVer = -1
db: Connection
CREATE_FILE_TABLE = """ CREATE TABLE IF NOT EXISTS files (
        uuid text NOT NULL,
        path text NOT NULL,
        hash text,
        metadata text,
        PRIMARY KEY (uuid, path)
    ); """

def createNewBackup(srcDir, backupDir):
    global backupVer, db

    backupVer = 0
    db = create_connection(backupDir + '/db.sqlite')
    with db:
        cur = db.cursor()
        cur.execute(CREATE_FILE_TABLE)
        recursiveAdd(srcDir, backupDir)
    if db:
        db.close()


def create_connection(db_file):
    """ create a database connection to a SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print("sqlite3 " + sqlite3.version + ": connected to " + db_file)
    except Error as e:
        print(e)

    return conn

def recursiveAdd(srcDir, backupDir):
    for item in os.scandir(srcDir):
        if item.is_dir():
            recursiveAdd(item, backupDir)
            continue
        destPath = backupDir + "/data/" + str(backupVer)
        print(destPath)
        if not os.path.exists(destPath):
            os.makedirs(destPath)
        fileUuid = str(uuid.uuid4())
        shutil.copy2(item.path, destPath + "/" + fileUuid)

        fileHash = file_sha256(item.path)

        cur = db.cursor()
        cur.execute(''' INSERT INTO files(uuid, path, hash, metadata) VALUES(?,?,?,?) ''',\
            (fileUuid, item.path, fileHash, 'na'))
        db.commit()
    pass

def file_sha256(filePath):
    h = hashlib.sha256()

    with open(filePath, 'rb') as file:
        while True:
            chunk = file.read(h.block_size)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest()

def updateBackup(backupDir):
    pass