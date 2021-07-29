# -*- coding: UTF-8 -*-
import requests
import csv
import sys
import datetime
import time
import os 
import json
from glob import glob

import apikeys
import mylib

pathInput = "data/input/"
pathMobilitiData = pathInput + "mobiliti-2016/"
pathOutput = "data/output/abstracts/" + ("_".join(mylib.sectors)).replace("/", "") + "/"
inputTsv = pathInput + 'candidates.tsv'
outputTsv = pathInput + "_".join(mylib.sectors).replace("/","") + "_withNames.tsv"
outputTsvWithAsnOutcomes = outputTsv.replace(".tsv","") + "_AsnOutcomes.tsv"
pathAsnDownload = "" # insert here the path where the candidates CVs (i.e. the pdf files) are stored 



def getAbstracts(dois):
	
	doisToSkip = list()
	
	contents = glob(pathOutput + '*.json')
	contents.sort()
	for filename_withPath in contents:
		with open(filename_withPath) as json_file:
			data = json.load(json_file)
			doi = data['abstracts-retrieval-response']['coredata']['prism:doi']
			eid = data['abstracts-retrieval-response']['coredata']['eid']
			doisToSkip.append(doi)
	
	for doi in dois:
		if doi not in doisToSkip:
			print ('Processing ' + doi)
			jsonAbs = mylib.getAbstract(doi, 'DOI')
			if jsonAbs is not None:
				mylib.saveJsonAbstract(jsonAbs,pathOutput)
				print ('\tSaved to file.')
			else:
				print ('\tNone -> not saved.')
		else:
			print ('Skipping doi ' + doi + ': already downloaded')



dois = mylib.getDoisSet(inputTsv)

getAbstracts(dois)

# Add authors names and surnames to the TSV file
mylib.addAuthorsNamesToTsv(inputTsv, outputTsv, pathMobilitiData)

# Add the ASN outcomes to the TSV file
mylib.addAsnOutcomesToTsv(mylib.sectors, outputTsv, outputTsvWithAsnOutcomes, pathAsnDownload)




