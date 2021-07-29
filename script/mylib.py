# -*- coding: UTF-8 -*-
import datetime
import requests
import csv
import ast
from glob import glob
import json
import sys
import re
from lxml import html
import os
import json
import urllib.parse

sectors = ['13/D1', '13/D2', '13/D3', '01/B1', '09/H1']

apiURL_Abstract = {
	'doi': 'https://api.elsevier.com/content/abstract/doi/',
	'eid': 'https://api.elsevier.com/content/abstract/eid/'
}

def getAbstract(doi, doiOrEid, apikeys, max_retry=2, retry_delay=1):
	
	retry = 0
	cont = True
	while retry < max_retry and cont:

		if doiOrEid.lower() not in ['doi','eid']:
			print ("ERROR in mylib.getAbstract(): allowed type of search are 'DOI' and 'EID'.")

		params = {'apikey':apikeys[0], 'httpAccept':'application/json'} #, 'view':'FULL'}
		doiEncoded = urllib.parse.quote(doi)
		r = requests.get(apiURL_Abstract[doiOrEid.lower()] + doiEncoded, params=params)
				
		if r.status_code == 429:
			print ("Quota exceeded for key " + apikeys.keys[0] + " - EXIT.")
			apikeys.keys.pop(0)
		
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
 
	json = r.json() 
	json['request-time'] = str(datetime.datetime.now().utcnow())
	return json


def checkAbsFormat(j):
	return True


def saveJsonAbstract(j, pathOutput):

	if (checkAbsFormat(j)):
		eid = j['abstracts-retrieval-response']['coredata']['eid']
		
		if not os.path.isdir(pathOutput):
			os.makedirs(pathOutput)
			
		counter = 1
		completepath = os.path.join(pathOutput, eid + '.json')

		with open(completepath, 'w') as outfile:
			json.dump(j, outfile, indent=3)
		
		return True

	else:
		return False


# Get authors' names and surnames from the cv PDFs
def addAuthorsNamesToTsv(tsvIn, tsvOut, pathPdf):
	res = "SESSIONE	FASCIA	SETTORE	BIBL?	ID CV	COGNOME	NOME	NUMERO DOI ESISTENTI	DOIS ESISTENTI	DOIS NON ESISTENTI	I1	I2	I3	SETTORE CONCORSUALE	SSD	S1	S2	S3\n"
	# Load TSV
	with open(tsvIn, newline='') as csvfile:
		spamreader = csv.DictReader(csvfile, delimiter='\t')
		for row in spamreader:
			if row['SETTORE'].replace('-','/') in sectors:
				idCv = row['ID CV']
				sessione = row['SESSIONE']
				fascia = row['FASCIA']
				settore = row['SETTORE']
				pdfBasePath = pathPdf + "quadrimestre-" + sessione + "/fascia-" + fascia + "/" + settore + "/CV/"
				pdfFullPath = pdfBasePath + idCv + "_*.pdf"
				contents = glob(pdfFullPath)
				contents.sort()
				if len(contents) != 1:
					print ("ERROR - NOT FOUND - sessione: %s, fascia: %s, settore: %s, idCv: %s" % (sessione, fascia, settore, idCv))
					sys.exit()
				filename = (contents[0].replace(pdfBasePath, ""))
				filenameSPlit = filename.split("_")
				idPdf = filenameSPlit[0]
				surnameList = list()
				nameList = list()
				for i in range(1,len(filenameSPlit)):
					if filenameSPlit[i].isupper():
						surnameList.append(filenameSPlit[i])
					else:
						nameList.append(filenameSPlit[i].replace(".pdf", ""))
				name = " ".join(nameList)
				surname = " ".join(surnameList)
				res += "\t".join([row["SESSIONE"], row["FASCIA"], row["SETTORE"], row["BIBL?"], row["ID CV"], surname, name, row["NUMERO DOI ESISTENTI"], row["DOIS ESISTENTI"], row["DOIS NON ESISTENTI"], row["I1"], row["I2"], row["I3"], row["SETTORE CONCORSUALE"], row["SSD"], row["S1"], row["S2"], row["S3"]]) + "\n"
			
	text_file = open(tsvOut, "w")
	text_file.write(res)
	text_file.close()


class AutoVivification(dict):
	"""Implementation of perl's autovivification feature."""
	def __getitem__(self, item):
		try:
			return dict.__getitem__(self, item)
		except KeyError:
			value = self[item] = type(self)()
			return value

def addAsnOutcomesToTsv(sectors, inputTsv, outputTsv, pathAsnDownload):
	esitiMap = AutoVivification()
	for sector in sectors:
		for quadrimestre in range(1,6):
			for fascia in range(1,3):
				htmlFile = pathAsnDownload + "quadrimestre-" + str(quadrimestre) + "/fascia-" + str(fascia) + "/" + sector.replace("/", "-") + "/" + sector.replace("/", "-") + "_risultati.html"
				#print (htmlFile)
				tree = html.parse(htmlFile)
				els = tree.xpath('//table[position()=last()]/tbody/tr')
				for el in els:
					linkPdfCv = el.xpath('td[3]/a/@href')
					if len(linkPdfCv) != 1:
						print ("XPATH ERROR")
						sys.exit()
					idCvEsito = linkPdfCv[0].split("/")[7]
					elEsito = el.xpath('td[7]')
					if len(elEsito) != 1:
						print ("XPATH ERROR")
						sys.exit()
					esito = re.sub(r'\s+', '', elEsito[0].text)
					esitiMap[sector.replace("/","-")][str(quadrimestre)][str(fascia)][idCvEsito] = esito
	
	res = "SESSIONE	FASCIA	SETTORE	BIBL?	ID CV	COGNOME	NOME	NUMERO DOI ESISTENTI	DOIS ESISTENTI	DOIS NON ESISTENTI	I1	I2	I3	SETTORE CONCORSUALE	SSD	S1	S2	S3	ESITO\n"
	with open(inputTsv, newline='') as csvfile:
		spamreader = csv.DictReader(csvfile, delimiter='\t')
		for row in spamreader:
			idCv = row["ID CV"]
			fascia = row["FASCIA"]
			quadrimestre = row["SESSIONE"]
			settore = row["SETTORE"]
			esito = esitiMap[settore][quadrimestre][fascia][idCv]
			res += "\t".join([row["SESSIONE"], row["FASCIA"], row["SETTORE"], row["BIBL?"], row["ID CV"], row["COGNOME"], row["NOME"], row["NUMERO DOI ESISTENTI"], row["DOIS ESISTENTI"], row["DOIS NON ESISTENTI"], row["I1"], row["I2"], row["I3"], row["SETTORE CONCORSUALE"], row["SSD"], row["S1"], row["S2"], row["S3"], esito]) + "\n"
			
	text_file = open(outputTsv, "w")
	text_file.write(res)
	text_file.close()
			

# Get Scopus authors' ids (using names in the TSV and JSON abstracts)
def addAuthorsIdScopusToTsv(tsvIn, folderAbstracts, doiEidMap, tsvOut):
	res = dict()
	with open(tsvIn, newline='') as csvfile:
		numFound = 0
		numNotFound = 0
		spamreader = csv.DictReader(csvfile, delimiter='\t')
		for row in spamreader:
			if row['SETTORE'].replace('-','/') in sectors:
				dois = ast.literal_eval(row['DOIS ESISTENTI'])
				idCv = row['ID CV']
				surnameCv = row["COGNOME"]
				nameCv = row["NOME"]
				
				res[idCv] = ''
				for doi in dois:
					try:
						eid = doiEidMap["doi-to-eid"][doi]
						with open(folderAbstracts + eid + ".json") as json_file:
							found = False
							data = json.load(json_file)
							affiliationData = data["abstracts-retrieval-response"]["item"]["bibrecord"]["head"]["author-group"]
							if type(affiliationData) is not list:
								affiliationData = [affiliationData]
							for affiliation in affiliationData:
								if found:
									break
								for author in affiliation["author"]:
									if found:
										break
									nameJson = author["ce:given-name"]
									surnameJson = author["ce:surname"]
									indexednameJson = author["ce:indexed-name"]
									auidJson = author["@auid"]
									if re.sub("[a-z]'", "", surnameCv.lower().replace(" ", "")) in re.sub("[a-z]'", "", indexednameJson.lower().replace(" ", "").replace("-","")):
										if res[idCv] == '':
											res[idCv] = auidJson
										elif res[idCV] != auidJson:
											print ("Multiple EIDS - " + idCv)
											
										found = True
										numFound += 1
							if not found:
								print ("Author not found: %s %s %s %s" % (idCv, nameCv, surnameCv, eid))
								numNotFound += 1
										
					except:
						print ("ERROR - CV: " + idCv + ", EID not found for DOI: " + doi)
		print (numFound)
		print (numNotFound)
		
		for idCv in res:
			if res[idCv] == "":
				print (idCv)
				
		strRes = "SESSIONE	FASCIA	SETTORE	BIBL?	ID CV	COGNOME	NOME	SCOPUS ID	NUMERO DOI ESISTENTI	DOIS ESISTENTI	DOIS NON ESISTENTI	I1	I2	I3	SETTORE CONCORSUALE	SSD	S1	S2	S3\n"
		with open(tsvIn, newline='') as csvfile:
			spamreader = csv.DictReader(csvfile, delimiter='\t')
			for row in spamreader:
				if row['SETTORE'].replace('-','/') in sectors:
					strRes += "\t".join([row["SESSIONE"], row["FASCIA"], row["SETTORE"], row["BIBL?"], row["ID CV"], row["COGNOME"], row["NOME"], res[row["ID CV"]], row["NUMERO DOI ESISTENTI"], row["DOIS ESISTENTI"], row["DOIS NON ESISTENTI"], row["I1"], row["I2"], row["I3"], row["SETTORE CONCORSUALE"], row["SSD"], row["S1"], row["S2"], row["S3"]]) + "\n"
			
		text_file = open(tsvOut, "w")
		text_file.write(strRes)
		text_file.close()


def getDoisSet(f):
	doisList = list()
	with open(f, newline='') as csvfile:
		spamreader = csv.DictReader(csvfile, delimiter='\t')
		for row in spamreader:
			if row['SETTORE'].replace('-','/') in sectors:
				doiTemp = ast.literal_eval(row['DOIS ESISTENTI'])
				doisList.extend(doiTemp)

	return set(doisList)


def doiEidMap(folder):
	res = { "doi-to-eid": {}, "eid-to-doi": {} }
	contents = glob(folder + '*.json')
	contents.sort()
	for filename_withPath in contents:
		with open(filename_withPath) as json_file:
			data = json.load(json_file)
			try:
				eid = data['abstracts-retrieval-response']['coredata']['eid']
				doi = data['abstracts-retrieval-response']['coredata']['prism:doi']
				res["doi-to-eid"][doi] = eid
				res["eid-to-doi"][eid] = doi
			except:
				print ('*** EID/DOI ERROR: ' + filename_withPath + " ***")	
	return res
