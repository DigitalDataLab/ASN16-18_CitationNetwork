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

from datetime import datetime
from scholarmetrics import hindex


database = {
	"01/B1": "../data/output/informatici_settori.db",
	"09/H1": "../data/output/informatici_settori.db",
	"13/D1": "../data/output/statistici_settori.db",
	"13/D2": "../data/output/statistici_settori.db",
	"13/D3": "../data/output/statistici_settori.db",
	"INFO": "../data/output/informatici.db",
	"STAT": "../data/output/statistici.db"
}

ASNtsvFiles_Indicatori = {
	"INFO": "../data/output/informatici_indicatoriPoggi_inCV.tsv",
	"STAT": "../data/output/statistici_indicatoriPoggi_inCV.tsv"
}

ASNtsvFiles_Titoli = {
	"INFO": "../data/output/informatici_titoliPoggi.tsv",
	"STAT": "../data/output/statistici_titoliPoggi.tsv"
}

sectors = ['01/B1','09/H1',"INFO","13/D1","13/D2","13/D3","STAT"]

# Session dates ASN 2016-2018: 2016-12-02 2017-04-03 2017-08-04 2017-12-05 2018-04-06
"""D.M. 7 giugno 2016, n. 120, Art. 2:
	...dal 1° gennaio rispettivamente del decimo anno (prima fascia) e del quinto anno (seconda fascia) precedente la scadenza del quadrimestre di presentazione della domanda;"""
ASN2016deadlines_i1 = {
	"start": {
		1: {1: "2006-01-01", 2: "2007-01-01", 3: "2007-01-01", 4: "2007-01-01", 5: "2008-01-01"},
		2: {1: "2011-01-01", 2: "2012-01-01", 3: "2012-01-01", 4: "2012-01-01", 5: "2013-01-01"}
	},
	"end": {1: "2016-12-02", 2: "2017-04-03", 3: "2017-08-04", 4: "2017-12-05", 5: "2018-04-06"}
}

ASN2016deadlines_i2_i3 = {
	"start": {
		1: {1: "2001-01-01", 2: "2002-01-01", 3: "2002-01-01", 4: "2002-01-01", 5: "2003-01-01"},
		2: {1: "2006-01-01", 2: "2007-01-01", 3: "2007-01-01", 4: "2007-01-01", 5: "2008-01-01"}
	},
	"end": {1: "2016-12-02", 2: "2017-04-03", 3: "2017-08-04", 4: "2017-12-05", 5: "2018-04-06"}
}

strDays = "+30"
jsonFolderCv = "../data/input/mobiliti/JSON/" 

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
		SELECT DISTINCT id, authorId, settore, quadrimestre, fascia, bibl, I1, I2, I3
		FROM curriculum
	"""

	cur = conn.cursor()
	cur.execute(query_publications)
	rows = cur.fetchall()
	return rows


def select_titoli(conn):

	query_publications = """
		SELECT DISTINCT id, authorId, settore, quadrimestre, fascia, titoliCounter.'titolo-01', titoliCounter.'titolo-02', titoliCounter.'titolo-03', titoliCounter.'titolo-04', titoliCounter.'titolo-05', titoliCounter.'titolo-06', titoliCounter.'titolo-07', titoliCounter.'titolo-08', titoliCounter.'titolo-09', titoliCounter.'titolo-10', titoliCounter.'titolo-11'
		FROM curriculum
		INNER JOIN titoliCounter
		ON
		  curriculum.id = titoliCounter.cvId
	"""

	cur = conn.cursor()
	cur.execute(query_publications)
	rows = cur.fetchall()
	return rows


def select_i1_articles(conn, cvid, quadrimestre, fascia, ASN2016deadlines,strDays,allOrCv):
	if allOrCv == "ALL":
		q = """
			SELECT DISTINCT curriculum.id, curriculum.authorId, publication.eid
			FROM curriculum
			INNER JOIN wroteRelation
			ON
			  curriculum.authorId = wroteRelation.authorId
			INNER JOIN publication
			ON
			  wroteRelation.eid = publication.eid
			WHERE curriculum.id = '{curriculumId}' AND 
			  publication.publicationDate BETWEEN date('{dateFrom}') AND date('{dateTo}', '{days} days') AND
			  ( publication.subtypeDescription = 'Article' OR
				publication.subtypeDescription = 'Article in Press' OR
				publication.subtypeDescription = 'Review' OR
				publication.subtypeDescription = 'Letter' OR
				publication.subtypeDescription = 'Note' OR
				publication.subtypeDescription = 'Short Survey' )
		"""
	elif allOrCv == "CV":
		q = """
			SELECT DISTINCT curriculum.id, curriculum.authorId, publication.eid
			FROM curriculum
			INNER JOIN wroteRelationIndexCv
			ON
			  curriculum.id = wroteRelationIndexCv.idCv
			INNER JOIN publication
			ON
			  wroteRelationIndexCv.eid = publication.eid
			WHERE 
			  curriculum.id = '{curriculumId}' AND 
			  publication.publicationDate BETWEEN date('{dateFrom}') AND date('{dateTo}', '{days} days') AND
			  ( publication.subtypeDescription = 'Article' OR
				publication.subtypeDescription = 'Article in Press' OR
				publication.subtypeDescription = 'Review' OR
				publication.subtypeDescription = 'Letter' OR
				publication.subtypeDescription = 'Note' OR
				publication.subtypeDescription = 'Short Survey' ) AND
			  wroteRelationIndexCv.indexPubIndCv NOT NULL
		"""		
	else:
		print ("ERROR in select_i1_articles(): unknown allOrCv parameter provided.")
		sys.exit()
	
	cur = conn.cursor()
	cur.execute(q.format(curriculumId=cvid, days=strDays,dateFrom=ASN2016deadlines["start"][fascia][quadrimestre], dateTo=ASN2016deadlines["end"][quadrimestre]))
	rows = cur.fetchall()
	return rows

def select_publications(conn, cvid, quadrimestre,fascia,ASN2016deadlines,strDays,allOrCv):
	if allOrCv == "ALL":
		q = """
			SELECT DISTINCT curriculum.id, curriculum.authorId, publication.eid, publication.publicationDate
			FROM curriculum
			INNER JOIN wroteRelation
			ON
			  curriculum.authorId = wroteRelation.authorId
			INNER JOIN publication
			ON
			  wroteRelation.eid = publication.eid
			WHERE curriculum.id = '{curriculumId}' AND
			  publication.publicationDate BETWEEN date('{dateFrom}') AND date('{dateTo}', '{days} days')
		"""
	elif allOrCv == "CV":
		q = """
			SELECT DISTINCT curriculum.id, curriculum.authorId, publication.eid, publication.publicationDate
			FROM curriculum
			INNER JOIN wroteRelationIndexCv
			ON
			  curriculum.id = wroteRelationIndexCv.idCv
			INNER JOIN publication
			ON
			  wroteRelationIndexCv.eid = publication.eid
			WHERE curriculum.id = '{curriculumId}' AND
			  publication.publicationDate BETWEEN date('{dateFrom}') AND date('{dateTo}', '{days} days') AND
			  wroteRelationIndexCv.indexPubIndCv NOT NULL
		"""
	else:
		print ("ERROR in select_publications(): unknown allOrCv parameter provided.")
		sys.exit()
	
	cur = conn.cursor()
	cur.execute(q.format(curriculumId=cvid, days=strDays, dateFrom=ASN2016deadlines["start"][fascia][quadrimestre], dateTo=ASN2016deadlines["end"][quadrimestre]))
	rows = cur.fetchall()
	return rows


def select_i2_numCitations(conn,eid,quadrimestre,fascia):
	q = """
		SELECT *
		FROM citationCount
		WHERE eid = '{pubId}'
	"""
	
	cur = conn.cursor()
	cur.execute(q.format(pubId=eid))
	rows = cur.fetchall()
	if len(rows) == 0:
		# missing citation record -> return 0
		print ("WARNING: select_i2_numCitations() - missing citation record for paper '%s' -> return 0 citations" % eid)
		return 0
	elif len(rows) != 1:
		print ("ERROR: select_i2_numCitations() - returned %d citation record for paper '%s' -> Exit." % (len(rows),eid))
		sys.exit()
	
	row = rows[0]
	citTot = row[2]
	citPre2000 = row[3]
	cit2000 = row[4]
	cit2001 = row[5]
	cit2002 = row[6]
	cit2003 = row[7]
	cit2004 = row[8]
	cit2005 = row[9]
	cit2006 = row[10]
	cit2007 = row[11]
	cit2008 = row[12]
	cit2009 = row[13]
	cit2010 = row[14]
	cit2011 = row[15]
	cit2012 = row[16]
	cit2013 = row[17]
	cit2014 = row[18]
	cit2015 = row[19]
	cit2016 = row[20]
	cit2017 = row[21]
	cit2018 = row[22]
	cit2019 = row[23]
	cit2020 = row[24]

	# Session dates ASN 2016-2018: 2016-12-02 2017-04-03 2017-08-04 2017-12-05 2018-04-06
	if fascia == 1:
		# citazions in the last 15 years
		if quadrimestre == 1:
			# citazions between 2001 and 2016
			cit = citTot - (citPre2000 + cit2000 + cit2016 + cit2017 + cit2018 + cit2019 + cit2020)
		elif quadrimestre == 5:
			# citazions between 2003 and 2018
			cit = citTot - (citPre2000 + cit2000 + cit2001 + cit2002 + cit2018 + cit2019 + cit2020)
		else:
			# citazions between 2002 and 2017
			cit = citTot - (citPre2000 + cit2000 + cit2001 + cit2017 + cit2018 + cit2019 + cit2020)
	else:
		# citazioni in the last 10 years
		if quadrimestre == 1:
			# citazions between 2006 and 2016
			cit = cit2006 + cit2007 + cit2008 + cit2009 + cit2010 + cit2011 + cit2012 + cit2013 + cit2014 + cit2015 
		elif quadrimestre == 5:
			# citazions between 2008 and 2018
			cit = cit2008 + cit2009 + cit2010 + cit2011 + cit2012 + cit2013 + cit2014 + cit2015 + cit2016 + cit2017 
		else:
			# citazions between 2007 and 2017
			cit = cit2007 + cit2008 + cit2009 + cit2010 + cit2011 + cit2012 + cit2013 + cit2014 + cit2015 + cit2016 
	return cit


def select_publications_citations(conn, cvid, quadrimestre,allOrCv):
	ASNdeadlines = {1: "2016-12-02", 2: "2017-04-03", 3: "2017-08-04", 4: "2017-12-05", 5: "2018-04-06"}
	if allOrCv == "ALL":
		q = """
			SELECT DISTINCT curriculum.id, curriculum.authorId, publication.eid, citationCount.totalCitations, citationCount.'2017', citationCount.'2018', citationCount.'2019', citationCount.'2020' 
			FROM curriculum
			INNER JOIN wroteRelation
			ON
			  curriculum.authorId = wroteRelation.authorId
			INNER JOIN publication
			ON
			  wroteRelation.eid = publication.eid
			INNER JOIN citationCount
			ON
			  publication.eid = citationCount.eid
			WHERE curriculum.id = '{curriculumId}' AND
			  publication.publicationDate <= '{endDate}'	  
		"""
	elif allOrCv == "CV":
		q = """SELECT DISTINCT curriculum.id, curriculum.authorId, publication.eid, citationCount.totalCitations, citationCount.'2017', citationCount.'2018', citationCount.'2019', citationCount.'2020' 
			FROM curriculum
			INNER JOIN wroteRelationIndexCv
			ON
			  curriculum.id = wroteRelationIndexCv.idCv
			INNER JOIN publication
			ON
			  wroteRelationIndexCv.eid = publication.eid
			INNER JOIN citationCount
			ON
			  publication.eid = citationCount.eid
			WHERE curriculum.id = '{curriculumId}' AND
			  publication.publicationDate <= '{endDate}' AND
			  wroteRelationIndexCv.indexPubIndCv NOT NULL
		"""
	else:
		print ("ERROR in select_publications_citations(): unknown allOrCv parameter provided.")
		sys.exit()
	cur = conn.cursor()
	cur.execute(q.format(curriculumId=cvid, endDate=ASNdeadlines[quadrimestre]))
	rows = cur.fetchall()
	return rows


def computeASN2016Indicators(dbfile, outputTsvIndicatori,allOrCv="ALL"):
	"""Art. 2
Valori-soglia degli indicatori per i candidati all'Abilitazione Scientifica Nazionale

1. In attuazione di quanto disposto dall'art. 1, comma 1, e con riferimento all'Allegato C del D.M. 7 giugno 2016, n. 120, sono definiti nella Tabella 1, relativamente ai candidati all'abilitazione scientifica nazionale per i settori bibliometrici, i valori-soglia, distintamente per la prima e per la seconda fascia, dei seguenti indicatori:

a)      il numero complessivo di articoli riportati nella domanda e pubblicati su riviste scientifiche contenute nelle banche dati internazionali "Scopus" e "Web of Science", rispettivamente nei dieci anni (prima fascia) e cinque anni (seconda fascia) precedenti, di seguito denominato "numero articoli". Per i candidati, ai fini del calcolo di tale indicatore, sono considerati gli articoli riportati nella domanda, pubblicati e rilevati nelle banche dati internazionali "Scopus" e "Web of Science - Core Collection" dal 1° gennaio rispettivamente del decimo anno (prima fascia) e del quinto anno (seconda fascia) precedente la scadenza del quadrimestre di presentazione della domanda;

b)      il numero di citazioni ricevute dalla produzione scientifica contenuta nella domanda, pubblicata e rilevata dalle banche dati internazionali "Scopus" e "Web of Science", rispettivamente nei quindici anni (prima fascia) e dieci anni (seconda fascia) precedenti, di seguito denominato "numero citazioni". Per i candidati, ai fini del calcolo di tale indicatore, sono considerate le citazioni della produzione scientifica contenuta nella domanda, pubblicata e rilevata nelle banche dati internazionali "Scopus" e "Web of Science - Core Collection" dal 1° gennaio rispettivamente del quindicesimo anno (prima fascia) e del decimo anno (seconda fascia) precedente la scadenza del quadrimestre di presentazione della domanda;

c)      l'indice h di Hirsch, calcolato sulla base delle citazioni rilevate dalle banche dati internazionali "Scopus" e "Web of Science" con riferimento alle pubblicazioni contenute nella domanda e pubblicate, rispettivamente, nei quindici anni (prima fascia) e dieci anni (seconda fascia) precedenti, di seguito denominato "Indice H". Per i candidati, ai fini del calcolo di tale indicatore, sono considerate le citazioni di cui alla lettera b) riferite alle pubblicazioni contenute nella domanda, pubblicate e rilevate nelle banche dati internazionali "Scopus" e "Web of Science - Core Collection" dal 1° gennaio rispettivamente del quindicesimo anno (prima fascia) e del decimo anno (seconda fascia) precedente la scadenza del quadrimestre di presentazione della domanda."""
	
	conn = create_connection(dbfile)
	with conn:
		rows = select_cvid(conn)
		diffTotal = 0
		resStr = "cvId\tauthorId Scopus\tsettore\tquadrimestre\tfascia\tI1 poggi (journals)\tI1 ASN\tI2 poggi (citazioni)\tI2 ASN\tI3 poggi (hIndex)\tI3 ASN\tDelta I1\tDelta I2\tDelta I3\n"
		for row in rows:
			cvid = row[0]
			print (cvid)
			authorId = row[1]
			settore = row[2]
			quadrimestre = row[3]
			fascia = row[4]
			bibl = row[5]
			i1 = row[6]
			i2 = row[7]
			i3 = row[8]
			
			# Session dates ASN 2016-2018: 2016-12-02 2017-04-03 2017-08-04 2017-12-05 2018-04-06	
			if quadrimestre != 47777:
				journals = select_i1_articles(conn, cvid, quadrimestre, fascia,ASN2016deadlines_i1,strDays, allOrCv)
				i1Computed = len(journals)
				diffI1 = i1Computed-i1
				diffTotal += diffI1
				
				publications = select_publications(conn,cvid,quadrimestre,fascia,ASN2016deadlines_i2_i3,strDays, allOrCv)
				i2Computed = 0
				for publication in publications:
					eid = publication[2]
					citazioniPaper = select_i2_numCitations(conn,eid,quadrimestre,fascia)
					i2Computed += citazioniPaper
				diffI2 = i2Computed-i2
				
				publicationsHindex = select_publications_citations(conn, cvid, quadrimestre, allOrCv)
				citationsArray = list()
				for publication in publicationsHindex:
					if quadrimestre == 1:
						totCitations = publication[3]
						cit2017 = publication[4]
						cit2018 = publication[5]
						cit2019 = publication[6]
						cit2020 = publication[7]
						
						citCurrent = totCitations - (cit2017 + cit2018 + cit2019 + cit2020)
						citationsArray.append(citCurrent)
					elif quadrimestre == 5:
						totCitations = publication[3]
						cit2018 = publication[5]
						cit2019 = publication[6]
						cit2020 = publication[7]
						
						citCurrent = totCitations - (cit2018 + cit2019 + cit2020)
						citationsArray.append(citCurrent)
					else:
						totCitations = publication[3]
						cit2019 = publication[6]
						cit2020 = publication[7]
						
						citCurrent = totCitations - (cit2019 + cit2020)
						citationsArray.append(citCurrent)
				i3Computed = hindex(citationsArray)
				resStr += "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n" % (cvid, authorId, settore, quadrimestre, fascia, i1Computed, i1, i2Computed, i2, i3Computed, i3, i1Computed-i1, i2Computed-i2, i3Computed-i3)
		print (diffTotal)

		with open(outputTsvIndicatori, "w") as text_file:
			text_file.write(resStr)
			

def extractTitoliFromDb(dbfile, outputTsvTitoli):
	conn = create_connection(dbfile)
	with conn:
		rows = select_titoli(conn)
		resStr = "cvid\tauthorId Scopus\tsettore\tquadrimestre\tfascia\ttitolo-01\ttitolo-02\ttitolo-03\ttitolo-04\ttitolo-05\ttitolo-06\ttitolo-07\ttitolo-08\ttitolo-09\ttitolo-10\ttitolo-11\n"
		for row in rows:
			resStr += ("\t".join(str(x) for x in row) + "\n")
		
		with open(outputTsvTitoli, "w") as text_file:
			text_file.write(resStr)


def select_pubsForUpdate(conn, cvid):
	query_publications = """
		SELECT publication.eid, publication.doi, publication.title, publication.venueName, wroteRelation.authorId
		FROM curriculum
		INNER JOIN wroteRelation
		ON
		  curriculum.authorId = wroteRelation.authorId
		INNER JOIN publication
		ON
		  wroteRelation.eid = publication.eid
		WHERE curriculum.id = '{curriculumId}'
	"""
	cur = conn.cursor()
	cur.execute(query_publications.format(curriculumId=cvid))
	rows = cur.fetchall()
	return rows


def lcs(X, Y, m, n): 
	L = [[0 for i in range(n + 1)]  
			for i in range(m + 1)]  
			  
	# Following steps build  
	# L[m+1][n+1] in bottom  
	# up fashion. Note that  
	# L[i][j] contains length  
	# of LCS of X[0..i-1] and Y[0..j-1] 
	for i in range(m + 1): 
		for j in range(n + 1): 
			if i == 0 or j == 0: 
				L[i][j] = 0
			elif X[i - 1] == Y[j - 1]: 
				L[i][j] = L[i - 1][j - 1] + 1
			else: 
				L[i][j] = max(L[i - 1][j],  
							  L[i][j - 1]) 
		# L[m][n] contains length of  
		# LCS for X[0..n-1] and Y[0..m-1] 
	return L[m][n] 
	  
# Returns cost of making X[]  
# and Y[] identical. costX is  
# cost of removing a character 
# from X[] and costY is cost  
# of removing a character from Y[] 
def findMinCost(X, Y, costX, costY): 
	  
	# Find LCS of X[] and Y[] 
	m = len(X) 
	n = len(Y) 
	len_LCS =lcs(X, Y, m, n) 
	  
	# Cost of making two strings  
	# identical is SUM of following two  
	# 1) Cost of removing extra   
	# characters from first string  
	# 2) Cost of removing extra  
	# characters from second string 
	return (costX * (m - len_LCS) +
			costY * (n - len_LCS)) 


def match(cvPub, scopusPubs,minTitleLength=18,maxDifferenceCost=15):
	#index_cvPub = cvPub["id"]
	temp = (" ".join(cvPub["rawcontent"]))
	text_cvPub = ''.join(x for x in temp if x.isalpha()).lower()
	
	for scopusPub in scopusPubs:
		# match DOI scopus in the cv 
		if scopusPub["doi"] is not None and str(scopusPub["doi"]) in temp:
			#return index_cvPub
			return scopusPub["eid"]

	for scopusPub in scopusPubs:
		scopusTitle = "".join(x for x in scopusPub["title"] if x.isalpha()).lower()
		try:
			scopusVenue = "".join(x for x in scopusPub["venue"] if x.isalpha()).lower()
			if scopusTitle in text_cvPub and scopusVenue in text_cvPub:
				return scopusPub["eid"]
		except:
			pass
		if scopusTitle in text_cvPub and len(scopusTitle) > minTitleLength:
			return scopusPub["eid"]
		
	for scopusPub in scopusPubs:
		scopusTitle = "".join(x for x in scopusPub["title"] if x.isalpha()).lower()
		try:
			titolo_cvPub = "".join(x for x in cvPub["parsed"]["titolo"] if x.isalpha()).lower()
			if findMinCost(titolo_cvPub, scopusTitle,1,1) < maxDifferenceCost and len(scopusTitle) > minTitleLength:
				return scopusPub["eid"]
		except:
			pass
	return None


def create_wroteRelation_withIndex(conn, record):
	"""
	Create a new wroteRelation record into the wroteRelationIndexCv table
	:param conn:
	:param record:
	:return: wroteRelationIndex id
	"""
	sql = ''' INSERT INTO wroteRelationIndexCv(idCv,authorId,eid,indexPubIndCv,totPubIndCv)
			  VALUES(?,?,?,?,?) '''
	cur = conn.cursor()
	cur.execute(sql, record)
	return cur.lastrowid


def removeEidFromScopusPubs(eidMatched,scopusPubs):
	for scopusPub in scopusPubs:
		if scopusPub["eid"] == eidMatched:
			scopusPubs.remove(scopusPub)
		
def updateDbPublicationInCv(dbfile):
	conn = create_connection(dbfile)
	with conn:
		sql_create_wroteRelationIndexCv_table = """ CREATE TABLE IF NOT EXISTS wroteRelationIndexCv (
										idCv integer NOT NULL,
										authorId integer NOT NULL,
										eid string,
										indexPubIndCv integer,
										totPubIndCv integer,
										FOREIGN KEY (eid) REFERENCES authorScopus(id),
										FOREIGN KEY (eid) REFERENCES publication(eid)
									); """
		create_table(conn, sql_create_wroteRelationIndexCv_table)
		
		candidati = select_cvid(conn)
		print (len(candidati))
		for candidato in candidati:
			idCv = str(candidato[0])
			authorId = candidato[1]
			settore = candidato[2]
			quadrimestre = str(candidato[3])
			fascia = str(candidato[4])
			
			folder = jsonFolderCv + "quadrimestre-" + quadrimestre + "/fascia-" + fascia + "/" + settore.replace("/","-") + "/CV/" + idCv
			contents = glob(folder + "_*.json")
			contents.sort()
			
			for filename in contents:
				with open(filename) as f:
					data = json.load(f)
					cvPubs = data["pubbs_ind"]
			num_cvPubs = len(cvPubs)
			cvPubs_notMatched = list(cvPubs)
			
			# get Scopus pubs
			scopusPubs = list()
			rows = select_pubsForUpdate(conn, idCv)
			for row in rows:
				eid = row[0]
				doi = row[1]
				title = row[2]
				venue = row[3]
				scopusPubs.append({'eid': eid, 'doi': doi, 'title': title, 'venue': venue})
			
			matches = dict()
			num_cvPubs_matched = 0
			foundEids = list()
			for cvPub in cvPubs:
				eidMatched = match(cvPub, scopusPubs)
				indexInCv = cvPub["id"]
				if eidMatched is not None: # and indexInCv != 0:
					num_cvPubs_matched += 1
					cvPubs_notMatched.remove(cvPub)

					if eidMatched in foundEids:
						print ("ERROR in updateDbPublicationInCv() - eid %s already matched, current index = %s." % (eidMatched, indexInCv))
						print (foundEids)
						print (matches[eidMatched])
						sys.exit()
					removeEidFromScopusPubs(eidMatched,scopusPubs)
					matches[eidMatched] = indexInCv
					foundEids.append(eidMatched)

			for eid in matches.keys():
				index = matches[eid]
				tupleWRI = (idCv,authorId,eid,index,num_cvPubs)
				create_wroteRelation_withIndex(conn,tupleWRI)
			
			for scopusPub in scopusPubs:
				eid = scopusPub["eid"]
				tupleWRI = (idCv,authorId,eid,None,num_cvPubs)
				create_wroteRelation_withIndex(conn,tupleWRI)
					
			for cvPub_notMatched in cvPubs_notMatched:
				print ("\t" + " ".join(cvPub_notMatched["rawcontent"]))
			

for sector in sectors:
	updateDbPublicationInCv(database[sector])
	
	computeASN2016Indicators(database[sector],ASNtsvFiles_Indicatori[sector],"CV")
	
	extractTitoliFromDb(database[sector],ASNtsvFiles_Titoli[sector])

