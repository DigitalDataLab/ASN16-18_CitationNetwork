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

q1 = """
	SELECT DISTINCT(id)
	FROM authorScopus
	"""

q1_settori = """
	SELECT id
	FROM authorScopus_{settore}
	"""

q2 = """
	SELECT authorScopus.id AS authorCiting,   wr2.authorId AS authorCited, count(citesRelation.eidCited)
	FROM authorScopus
	INNER JOIN wroteRelation
	  ON authorScopus.id = wroteRelation.authorId
	INNER JOIN citesRelation
	  ON wroteRelation.eid = citesRelation.eidCiting
	INNER JOIN wroteRelation AS wr2
	  ON citesRelation.eidCited = wr2.eid
	WHERE 
	  authorScopus.id = ? AND
	  date(citesRelation.citationDate) < date('2018-04-07')
	GROUP BY authorCited
	ORDER BY authorCited
	"""

q2_settori = """
	SELECT authorScopus_{settore}.id AS authorCiting,   wr2.authorId AS authorCited, count(citesRelation.eidCited)
	FROM authorScopus_{settore}
	INNER JOIN wroteRelation
	  ON authorScopus_{settore}.id = wroteRelation.authorId
	INNER JOIN citesRelation
	  ON wroteRelation.eid = citesRelation.eidCiting
	INNER JOIN wroteRelation AS wr2
	  ON citesRelation.eidCited = wr2.eid
	INNER JOIN authorScopus_{settore} AS as2
	  ON wr2.authorId = as2.id
	WHERE 
	  authorScopus_{settore}.id = {auid} AND
	  date(citesRelation.citationDate) < date('2018-04-07')
	GROUP BY authorCited
	ORDER BY authorCited
	"""

def select_authors(conn,sector='ALL'):
	cur = conn.cursor()
	if sector == 'ALL':
		cur.execute(q1)
	else:
		cur.execute(q1_settori.format(settore=sector.replace("/","")))
 
	rows = cur.fetchall()

	return rows


def select_citatinos(conn,authorId,sector='ALL'):
	cur = conn.cursor()
	if sector == 'ALL':
		cur.execute(q2, (authorId,))
	else:
		cur.execute(q2_settori.format(auid=authorId,settore=sector.replace("/","")))
		
	rows = cur.fetchall()

	return rows

class AutoVivification(dict):
	"""Implementation of perl's autovivification feature."""
	def __getitem__(self, item):
		try:
			return dict.__getitem__(self, item)
		except KeyError:
			value = self[item] = type(self)()
			return value

#Save Adjacency Matrix to TSV file
def saveMatrixToFile(tsv, matrix, authorList):
	res = "\t" + "\t".join(authorList) + "\n"
	for authorIdCiting in authorList:
		line = list()
		for authorIdCited in authorList:
			line.append(str(matrix[authorIdCiting][authorIdCited]))
		res += authorIdCiting + "\t" + "\t".join(line) + "\n"
	text_file = open(tsv, "w")
	text_file.write(res)
	text_file.close()

sectors = ['01/B1','09/H1',"INFO","13/D1","13/D2","13/D3","STAT"]
database = {
	"01/B1": "../data/output/informatici_settori.db",
	"09/H1": "../data/output/informatici_settori.db",
	"13/D1": "../data/output/statistici_settori.db",
	"13/D2": "../data/output/statistici_settori.db",
	"13/D3": "../data/output/statistici_settori.db",
	"INFO": "../data/output/informatici.db",
	"STAT": "../data/output/statistici.db"
}
tsv = {
	"01/B1": "../data/output/adjacencyMatrix_01B1_pre20180407.tsv",
	"09/H1": "../data/output/adjacencyMatrix_09H1_pre20180407.tsv",
	"13/D1": "../data/output/adjacencyMatrix_13D1_pre20180407.tsv",
	"13/D2": "../data/output/adjacencyMatrix_13D2_pre20180407.tsv",
	"13/D3": "../data/output/adjacencyMatrix_13D3_pre20180407.tsv",
	"INFO": "../data/output/adjacencyMatrix_informatici_pre20180407.tsv",
	"STAT": "../data/output/adjacencyMatrix_statistici_pre20180407.tsv"
}
	
def main():

	for sector in sectors:
		
		print (sector)
		authorIds = list()
		# create a database connection
		conn = create_connection(database[sector])
		
		with conn:
			if sector in ["INFO","STAT"]:
				rows = select_authors(conn)
			else:
				rows = select_authors(conn,sector)
				
			for row in rows:
				authorIds.append(str(row[0]))
			
			# initialize empty map (dict of dict)
			matrix = AutoVivification()
			for authorId1 in authorIds:
				for authorId2 in authorIds:
					matrix[authorId1][authorId2] = 0

			counter = 1
			for authorId in authorIds:
				if sector in ["INFO","STAT"]:
					rowsCit = select_citatinos(conn,authorId)
				else:
					rowsCit = select_citatinos(conn,authorId,sector)
				
				for rowCit in rowsCit:
					citing = str(rowCit[0])
					cited = str(rowCit[1])
					numCit = int(rowCit[2])
					oldValue = matrix[citing][cited]
					matrix[citing][cited] = oldValue + numCit
				print ("%d/%d" % (counter, len(authorIds)))
				counter += 1
		
			saveMatrixToFile(tsv[sector], matrix, authorIds)
	
if __name__ == '__main__':
	main()
