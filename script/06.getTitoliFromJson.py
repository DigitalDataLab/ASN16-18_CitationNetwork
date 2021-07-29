import sqlite3
from sqlite3 import Error
from glob import glob
import json
import csv
import sys
import os
import string 

import datetime
import time
import requests
import ast
from glob import glob
import re
from lxml import html
import urllib.parse

import conf
import apikeys

database = {
	"01/B1": "../data/output/informatici_settori.db",
	"09/H1": "../data/output/informatici_settori.db",
	"13/D1": "../data/output/statistici_settori.db",
	"13/D2": "../data/output/statistici_settori.db",
	"13/D3": "../data/output/statistici_settori.db",
	"INFO": "../data/output/informatici.db",
	"STAT": "../data/output/statistici.db"
}

sectors = ['01/B1','09/H1',"INFO","13/D1","13/D2","13/D3","STAT"]
pathInput = "../data/input/titoli-json/"
fileTitoli = "../data/input/titoli_2016.json"

def create_connection(db_file):
	""" create a database connection to the SQLite database
		specified by the db_file
	:param db_file: database file
	:return: Connection object or None
	"""
	conn = None
	try:
		conn = sqlite3.connect(db_file)
	except Error as e:
		print(e)
 
	return conn


def create_table(conn, create_table_sql):
	""" create a table from the create_table_sql statement
	:param conn: Connection object
	:param create_table_sql: a CREATE TABLE statement
	:return:
	"""
	try:
		c = conn.cursor()
		c.execute(create_table_sql)
	except Error as e:
		print(e)


def select_cvid(conn):

	query_publications = """
		SELECT DISTINCT id
		FROM curriculum
	"""

	cur = conn.cursor()
	cur.execute(query_publications)
	rows = cur.fetchall()
	return rows


def create_titoli(conn, titoliTuple):
	"""
	Create a new record into the titoli table
	:param conn:
	:param titoliTuple:
	:return: titoliRecord id
	"""
	sql = ''' INSERT INTO titoliCounter('cvid', 'titolo-01', 'titolo-02','titolo-03','titolo-04','titolo-05','titolo-06','titolo-07','titolo-08','titolo-09','titolo-10','titolo-11')
			  VALUES(?,?,?,?,?,?,?,?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, titoliTuple)
	return cur.lastrowid

def create_titoliDescrizione(conn, tupleTitoliDescrizione):
	"""
	Create a new record into the titoliDescrizione table
	:param conn:
	:param tupleTitoliDescrizione:
	:return: titoliDescrizione id
	"""
	sql = ''' INSERT INTO titoliDescrizione('id', 'descrizione')
			  VALUES(?,?) '''
	cur = conn.cursor()
	cur.execute(sql, tupleTitoliDescrizione)
	return cur.lastrowid


def loadTitoliFromJson(fileTitoli):
	translator = str.maketrans('', '', string.punctuation)
	with open(fileTitoli) as f:
		data = json.load(f)
		titoli = data["titoli"]
		titoli_stringToIndex = dict()
		titoli_stringToIndex["paroparo"] = dict()
		titoli_stringToIndex["replaced"] = dict()
		for index in titoli.keys():
			titoli_stringToIndex["paroparo"][titoli[index]] = index
			titoli_stringToIndex["replaced"][''.join(e for e in titoli[index].replace("’","'").replace("à","a").replace("è","e").replace("é","e").replace("ì","i").replace("ò","o").replace("ù","u") if e.isalnum())] = index
		return titoli_stringToIndex


def getIndexTitolo(searchString,titoli_stringToIndex):
	translator = str.maketrans('', '', string.punctuation)
	searchString_replaced = ''.join(e for e in searchString.replace("’","'").replace("à","a").replace("è","e").replace("é","e").replace("ì","i").replace("ò","o").replace("ù","u") if e.isalnum())
	found = False
	for titolo in titoli_stringToIndex["replaced"].keys():
		if searchString_replaced in titolo.translate(translator):
			return titoli_stringToIndex["replaced"][titolo]
	if not found:
		print ("ERROR: titolo not found. Exit.")
		sys.exit()		

	
for sector in sectors:
	# Write titoliDescrizione into the DB
	conn = create_connection(database[sector])
	with conn:
		q = """
			CREATE TABLE IF NOT EXISTS 'titoliDescrizione' (
				'id' string NOT NULL PRIMARY KEY,
				'descrizione' string NOT NULL
		  );
		"""
		create_table(conn, q)
		
		with open(fileTitoli) as f:
			data = json.load(f)
			for index in data["titoli"].keys():
				create_titoliDescrizione(conn, ("titolo-"+str(index),data["titoli"][index]))
	
	# Write titoli into the DB
	conn = create_connection(database[sector])
	with conn:
		create_table_titoli = """
			CREATE TABLE IF NOT EXISTS titoliCounter (
				cvId integer NOT NULL PRIMARY KEY,
				'titolo-01' integer NOT NULL,
				'titolo-02' integer NOT NULL,
				'titolo-03' integer NOT NULL,
				'titolo-04' integer NOT NULL,
				'titolo-05' integer NOT NULL,
				'titolo-06' integer NOT NULL,
				'titolo-07' integer NOT NULL,
				'titolo-08' integer NOT NULL,
				'titolo-09' integer NOT NULL,
				'titolo-10' integer NOT NULL,
				'titolo-11' integer NOT NULL,
				FOREIGN KEY (cvId) REFERENCES curriculum(id)
			  );
		"""
		create_table(conn, create_table_titoli)
				
		titoli_stringToIndex = loadTitoliFromJson(fileTitoli)
		rows = select_cvid(conn)
		for row in rows:
			cvid = str(row[0])
			
			titoliDict = dict()
			for i in range(1,12):
				titoliDict[i] = 0
			
			contents = glob(pathInput + cvid + "_*.json")
			contents.sort()
			if len(contents) != 1:
				print ("ERROR: cvid = %s - json file not found. Exit." % cvid)
				sys.exit()
			for fileJson in contents:
				with open(fileJson) as f:
					data = json.load(f)
					titoli = data["titoli"].keys()
					for titolo in titoli:
						index = getIndexTitolo(titolo,titoli_stringToIndex)
						numTitoli = len(data["titoli"][titolo])
						titoliDict[index] = numTitoli
			titoliTuple = (int(cvid),int(titoliDict['01']),int(titoliDict['02']),int(titoliDict['03']),int(titoliDict['04']),int(titoliDict['05']),int(titoliDict['06']),int(titoliDict['07']),int(titoliDict['08']),int(titoliDict['09']),int(titoliDict['10']),int(titoliDict['11']))
			create_titoli(conn,titoliTuple)
			
