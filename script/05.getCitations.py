import sqlite3
from sqlite3 import Error
from glob import glob
import json
import csv
import sys
import os


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
tsv = {
	"01/B1": "../data/output/adjacencyMatrix_01B1_pre20180407.tsv",
	"09/H1": "../data/output/adjacencyMatrix_09H1_pre20180407.tsv",
	"13/D1": "../data/output/adjacencyMatrix_13D1_pre20180407.tsv",
	"13/D2": "../data/output/adjacencyMatrix_13D2_pre20180407.tsv",
	"13/D3": "../data/output/adjacencyMatrix_13D3_pre20180407.tsv",
	"INFO": "../data/output/adjacencyMatrix_informatici_pre20180407.tsv",
	"STAT": "../data/output/adjacencyMatrix_statistici_pre20180407.tsv"
}

sectors = ['01/B1','09/H1',"INFO","13/D1","13/D2","13/D3","STAT"]
pathOutput = "../data/output/citation-overview/"

fileOutput = "asnCitationNetwork"
dateInterval = "2000-2020"
eidGroupSize = 25

apiURL_CitationOverview = "https://api.elsevier.com/content/abstract/citations"

def getCitationOverview(eidOrEids, date, max_retry=2, retry_delay=1):
	
	time.sleep(0.5)
	
	if type(eidOrEids) is list:
		eid = ",".join(eidOrEids)
	else:
		eid = eidOrEids
	
	retry = 0
	cont = True
	while retry < max_retry and cont:

		params = {"apikey":apikeys.apikey_poggi, "insttoken":apikeys.institutionalToken_poggi, "httpAccept":"application/json", "scopus_id":eid,"date":date}
		r = requests.get(apiURL_CitationOverview, params=params)
		
		if r.status_code == 429:
			print ("Quota exceeded for key " + apikeys.keys[0] + " - EXIT.")
			
		elif r.status_code > 200 and r.status_code < 500:
			print(u"{}: errore nella richiesta: {}".format(r.status_code, r.url))
			return None

		if r.status_code != 200:
			retry += 1
			if retry < max_retry:
				time.sleep(retry_delay)
			continue

		cont = False 
			 
	if retry >= max_retry: 
		return None 
 
	j = r.json() 
	j['request-time'] = str(datetime.datetime.now().utcnow())
	return j	


def saveJsonCitationOverview(j, filename, pathOutput):
	
	if not os.path.isdir(pathOutput):
		os.makedirs(pathOutput)
	completepath = os.path.join(pathOutput, filename + '.json')
	with open(completepath, 'w') as outfile:
		json.dump(j, outfile, indent=3)
	return True


def downloadCitationOverviewJson(dbFilename,groupSize,date,filename,path):

	conn = create_connection(dbFilename)
	
	eids_wrote = list()
	with conn:
		eids = list()
		rows = select_eids_all(conn)
		for row in rows:
			eid = row[0]
			eids.append(eid)
	
	contents = glob(path + filename + "*.json")
	contents.sort()
	indexMax = 0
	for fileDownloaded in contents:
		index = int(fileDownloaded.split("_")[1].replace(".json",""))
		if index > indexMax:
			indexMax = index
	print (indexMax)
	
	eidGroups = [eids[k:k+groupSize] for k in range(0, len(eids), groupSize)]
	numDownloaded = 0
	for eidGroup in eidGroups:
		if numDownloaded < indexMax:
			numDownloaded += 1
			continue
		
		j = getCitationOverview(eidGroup,date)
		
		if j is not None and saveJsonCitationOverview(j, filename + "_" + str(numDownloaded+1), path):
			numDownloaded += 1
			print ('Saved to file.')
		else:
			print ('None -> not saved (i.e. not found or json already downloaded).')
		print ("%d queries remaining" % (((len(eids)//eidGroupSize)+1)-numDownloaded))	


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


def select_eids(conn):

	query_publications = """
		SELECT DISTINCT publication.eid
		FROM cercauniversita
		INNER JOIN wroteRelation
		ON
		  cercauniversita.authorId = wroteRelation.authorId
		INNER JOIN publication
		ON
		  wroteRelation.eid = publication.eid
		WHERE cercauniversita.authorId is NOT ''
		ORDER BY publication.eid ASC
	""" 
	
	cur = conn.cursor()
	cur.execute(query_publications)
	rows = cur.fetchall()
	return rows

def select_eids_all(conn):

	query_publications = """
		SELECT DISTINCT eid
		FROM publication
		WHERE eid is NOT ''
		ORDER BY eid ASC
	"""
	
	cur = conn.cursor()
	cur.execute(query_publications)
	rows = cur.fetchall()
	return rows

def create_citationRecord(conn, citationTuple):
	"""
	Create a new citation record into the citation table
	:param conn:
	:param citationTuple:
	:return: citationRecord id
	"""
	sql = ''' INSERT INTO citationCount(eid, doi, totalCitations,pre2000,'2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020')
			  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, citationTuple)
	return cur.lastrowid

def saveJsonCitationOverviewToDb(pathJsonFolder,dbFilename):

	create_table_citation = """CREATE TABLE IF NOT EXISTS citationCount (
										eid text NOT NULL PRIMARY KEY,
										doi text,
										totalCitations integer NOT NULL,
										pre2000 integer NOT NULL,
										'2000' integer NOT NULL,
										'2001' integer NOT NULL,
										'2002' integer NOT NULL,
										'2003' integer NOT NULL,
										'2004' integer NOT NULL,
										'2005' integer NOT NULL,
										'2006' integer NOT NULL,
										'2007' integer NOT NULL,
										'2008' integer NOT NULL,
										'2009' integer NOT NULL,
										'2010' integer NOT NULL,
										'2011' integer NOT NULL,
										'2012' integer NOT NULL,
										'2013' integer NOT NULL,
										'2014' integer NOT NULL,
										'2015' integer NOT NULL,
										'2016' integer NOT NULL,
										'2017' integer NOT NULL,
										'2018' integer NOT NULL,
										'2019' integer NOT NULL,
										'2020' integer NOT NULL,
										FOREIGN KEY (eid) REFERENCES publication(eid)
									  );"""

	conn = create_connection(dbFilename)
	create_table(conn,create_table_citation)
	
	eidsFromDb = set()
	with conn:
		rows = select_eids_all(conn)
		for row in rows:
			eid = row[0]
			eidsFromDb.add(eid)
		print (len(eidsFromDb))
				
		contents = glob(pathJsonFolder + "*.json")
		contents.sort()
		
		for fileDownloaded in contents:
			print (fileDownloaded)
			with open(fileDownloaded) as f:
				data = json.load(f)
				
				years = list()
				for year in data["abstract-citations-response"]["citeColumnTotalXML"]["citeCountHeader"]["columnHeading"]:
					years.append(year["$"])
				
				eidDoiMap = dict()
				for paper in data["abstract-citations-response"]["identifier-legend"]["identifier"]:
					doi = paper["prism:doi"]
					eid = "2-s2.0-" + paper["scopus_id"]
					eidDoiMap[eid] = doi
				
				for paper in data["abstract-citations-response"]["citeInfoMatrix"]["citeInfoMatrixXML"]["citationMatrix"]["citeInfo"]:
					eid = paper["dc:identifier"].replace("SCOPUS_ID:","2-s2.0-")
					if eid not in eidsFromDb:
						print ("Error: %s not in the DB" % eid)
						sys.exit()
					else:
						eidsFromDb.remove(eid)
					
					rangeCount = paper["rangeCount"]
					total = paper["rowTotal"]
					
					citations = dict()
					citations["total"] = total
					citations["before2000"] = int(total)-int(rangeCount)
					
					i = 0
					for citationPerYear in paper["cc"]:
						numCitation = citationPerYear["$"]
						citations[years[i]] = numCitation
						i += 1
					
					citationTuple = (eid,eidDoiMap[eid],citations["total"],citations["before2000"],citations["2000"],citations["2001"],citations["2002"],citations["2003"],citations["2004"],citations["2005"],citations["2006"],citations["2007"],citations["2008"],citations["2009"],citations["2010"],citations["2011"],citations["2012"],citations["2013"],citations["2014"],citations["2015"],citations["2016"],citations["2017"],citations["2018"],citations["2019"],citations["2020"])
					print (create_citationRecord(conn, citationTuple))
					

for sector in sectors:
	downloadCitationOverviewJson(database[sector], eidGroupSize, dateInterval,fileOutput,pathOutput)
	saveJsonCitationOverviewToDb(pathOutput,database[sector])

