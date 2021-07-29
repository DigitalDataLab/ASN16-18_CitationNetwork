import sqlite3
from sqlite3 import Error
from glob import glob
import json
import csv
import sys
import os

import conf

def create_connection(db_file):
	""" create a database connection to the SQLite database
		specified by db_file
	:param db_file: database file
	:return: Connection object or None
	"""
	conn = None
	try:
		conn = sqlite3.connect(db_file)
		return conn
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

def create_author(conn, author):
	"""
	Create a new author into the authorScopus table
	:param conn:
	:param author:
	:return: author id
	"""
	sql = ''' INSERT INTO authorScopus(id,givenname,surname,initials,orcid)
			  VALUES(?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, author)
	return cur.lastrowid
	

def main():
	# create a database connection
	conn = create_connection(conf.dbFilename_sectors)
	with conn:

		for sector in conf.sectors:
			
			print (sector)
			query1 = """CREATE TABLE IF NOT EXISTS authorScopus_{settore} (
							id integer PRIMARY KEY,
							givenname text,
							surname text,
							initials text,
							orcid text
						);"""
			create_table(conn, query1.format(settore=sector.replace("/","")))
 
			query2 = '''SELECT authorId
				FROM cercauniversita
				WHERE 
				  authorId <> '' AND
				  settore = '{settore}' 
				UNION
				select authorId
				FROM curriculum
				WHERE 
				  authorId NOT LIKE 'MISSING%' AND
				  settore = '{settore}'
				 ORDER BY authorId'''
			cur = conn.cursor()
			rows = cur.execute(query2.format(settore=sector))
			for row in rows:
				authorId = row[0]
				cur = conn.cursor()
				cur.execute("SELECT * FROM authorScopus WHERE id = ?", (authorId,))
				rows2 = cur.fetchall()
				if len(rows2) != 1:
					print ("ERROR")
					sys.exit()
				query3 = 'INSERT into authorScopus_{settore}(id,givenname,surname,initials,orcid) values ({idaut},"{givenname}","{surname}","{initials}","{orcid}")'
				cur.execute((query3.format(settore=sector.replace("/",""),idaut=int(authorId), givenname=rows2[0][1],surname=rows2[0][2],initials=rows2[0][3],orcid=rows2[0][4])))
				
				
if __name__ == '__main__':
	main()
