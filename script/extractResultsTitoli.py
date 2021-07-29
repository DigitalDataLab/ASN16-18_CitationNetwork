from lxml import etree
from io import StringIO, BytesIO
import sys
import glob
import csv
import sqlite3
from sqlite3 import Error
import copy

class AutoVivification(dict):
	"""Implementation of perl's autovivification feature."""
	def __getitem__(self, item):
		try:
			return dict.__getitem__(self, item)
		except KeyError:
			value = self[item] = type(self)()
			return value

def readFile(fname):
	with open(fname,"r") as f:
		return f.read()
		
def noSoglie(lines):
	txt = " ".join(lines).lower().replace("\n","")
	if "non supera le soglie" in txt or "non raggiunge almeno 2 valori soglia" in txt or "non raggiungono almeno 2 valori soglia" in txt:
		return True
	return False



def computeAndSaveTitoli(inputPath, outputFile):
	res = AutoVivification()
	
	for q in ["1","2","3","4", "5"]:
		for f in ["1","2"]:
			for s in ["01-B1","09-H1","13-D1","13-D2","13-D3"]:
				for txtname in glob.glob(inputPath % (q,f,s)):
					cvId = txtname.split("/")[8].split("_")[0]
					
					if s == "01-B1":
						with open(txtname, 'r') as myfile:
							lines = myfile.readlines()
							txt = " ".join(lines).lower().replace("\n","")
							
							if noSoglie(lines):
								res[q][f][s][cvId]["titoli"]["Soglie"] = "No"
								continue
							elif "scientifica raggiungono " in txt:
								res[q][f][s][cvId]["titoli"]["Soglie"] = "Sì"
							else:
								print (txtname + ": ERROR soglia informatici.")
								sys.exit()
									
						with open(txtname, 'r') as myfile:
							txtNWS = ""
							titoli = ""
							titoliNWS = ""
							startOk = False
							endOk = False
							for line in myfile:
								lineNWS = " ".join(line.split())
								txtNWS += lineNWS + " "
								if "TITOLI." in line:
									titoli += line + "\n"
									startOk = True
								elif "PUBBLICAZIONI SCIENTIFICHE." in line and startOk:
									endOk = True
									break
								elif startOk:
									titoli += line
							
							if not (startOk and endOk):
								print (txtname + ": ERRORE sezione titoli non riconosciuta")
								sys.exit()
							else:
								for titoliLine in titoli.split("\n"):
									titoliNWS += " ".join(titoliLine.split()) + " "
								
								if "accertato il possesso di almeno tre titoli, tra " in titoliNWS:
									index = titoliNWS.find("tra cui ")
									if index == -1:
										index = titoliNWS.find("tra i quali: ")
									
									if index != -1:
										arrTemp = titoliNWS[index+7:].lower().split(" titolo ")[1:]
										if len(arrTemp) == 3:
											for temp in arrTemp:
												titolo = temp.replace(" e","").replace(" ","").replace(".","").replace(",","")
												if len(titolo) != 1:
													print (txtname + ": ERROR1.")
													sys.exit()
												res[q][f][s][cvId]["titoli"][titolo] = "Sì"
										else:
											print (txtname + ": ERROR2.")
											sys.exit()
									else:
										print (titoliNWS)
								elif "non accertato il possesso di almeno tre titoli." in titoliNWS:
									for t in titoliNWS.lower().split("titolo ")[1:]:
										if "sufficient" not in t:
											if "adeguata" in t and (t[:t.find(" adeguat")].split()[-2]) == "non":
												print (txtname + ": ERROR3.")
												sys.exit()
											else:
												titolo = (t.split(":")[0])
												if len(titolo) != 1:
													print (txtname + ": ERROR4.")
													sys.exit()
												else:
													res[q][f][s][cvId]["titoli"][titolo] = "Sì"
										else:
											if (t[:t.find(" sufficient")].split()[-2]) != "non":
												print (txtname + ": ERROR5.")
												sys.exit()
											else:
												titolo = (t.split(":")[0])
												res[q][f][s][cvId]["titoli"][titolo] = "No"
								else:
									print (txtname + ": " + titoli)
						
						for titolo in ["a","b","c","d","e","f","g","h","i","l"]:
							if titolo not in res[q][f][s][cvId]["titoli"]:
								res[q][f][s][cvId]["titoli"][titolo] = "?"
						continue
						
					with open(txtname, 'r') as myfile:
						lines = myfile.readlines()
						if noSoglie(lines):
							res[q][f][s][cvId]["titoli"]["Soglie"] = "No"
							continue
						else:
							res[q][f][s][cvId]["titoli"]["Soglie"] = "Sì"
						
						if s == "09-H1":
							arrtitoli = ["a","c","d","e","f","g","i"]
							for i in range(26,33):
								esito = lines[i].replace("\n","")
								if esito in ["Sì","No"]:
									res[q][f][s][cvId]["titoli"][arrtitoli[i-26]] = esito
								else:
									res["errori"][cvId] = txtname
									break
						
						elif s == "13-D1":
							arrtitoli = ["a","b","c","d","e","f","g","h","l"]
							for i in range(30,39):
								esito = lines[i].replace("\n","")
								if esito in ["Sì","No"]:
									res[q][f][s][cvId]["titoli"][arrtitoli[i-30]] = esito
								else:
									res["errori"][cvId] = txtname
									break
						
						elif s == "13-D2":
							arrtitoli = ["a","b","c","d","e","f","g","h","i","l"]
							for i in range(32,42):
								esito = lines[i].replace("\n","")
								if esito in ["Sì","No"]:
									res[q][f][s][cvId]["titoli"][arrtitoli[i-32]] = esito
								else:
									res["errori"][cvId] = txtname
									break
						
						elif s == "13-D3":
							arrtitoli = ["a","b","c","d","e","f","g","h"]
							for i in range(28,36):
								esito = lines[i].replace("\n","")
								if esito in ["Sì","No"]:
									res[q][f][s][cvId]["titoli"][arrtitoli[i-28]] = esito
								else:
									res["errori"][cvId] = txtname
									break
		
	table = "QUADRIMESTRE\tFASCIA\tSETTORE\tCVID\tI1\tI2\tI3\tI4\tI5\tI6\tI7\tI8\tI9\tI10\tSuperagrep 75327 Soglie\n"	
	for q in res:
		if q == "errori":
			continue
		for f in res[q]:
			for s in res[q][f]:
				for cvId in res[q][f][s]:
					temp = list()
					for titolo in ["a","b","c","d","e","f","g","h","i","l"]:
						if titolo in res[q][f][s][cvId]["titoli"]:
							esito = res[q][f][s][cvId]["titoli"][titolo]
						else:
							esito = "X"
						temp.append(esito)
					table += "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (q,f,s,cvId,temp[0],temp[1],temp[2],temp[3],temp[4],temp[5],temp[6],temp[7],temp[8],temp[9],res[q][f][s][cvId]["titoli"]["Soglie"])
					
	with open(outputFile, 'w') as f:
		f.write(table)


computeAndSaveTitoli("../data/input/mobiliti/quadrimestre-%s/fascia-%s/%s/Giudizi-TXT/*_giudizi.txt", "giudizi_v2.csv")



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

def select_cvid_scopus(conn):

	query_publications = """
		SELECT DISTINCT id, authorId, settore
		FROM curriculum
	"""

	cur = conn.cursor()
	cur.execute(query_publications)
	rows = cur.fetchall()
	return rows
	
database = {
	"01/B1": "../data/output/informatici_settori.db",
	"09/H1": "../data/output/informatici_settori.db",
	"13/D1": "../data/output/statistici_settori.db",
	"13/D2": "../data/output/statistici_settori.db",
	"13/D3": "../data/output/statistici_settori.db",
	"INFO": "../data/output/informatici.db",
	"STAT": "../data/output/statistici.db"
}

mapping = dict()
for tipo in ["INFO", "STAT"]:
	conn = create_connection(database[tipo])
	with conn:
		rows = select_cvid_scopus(conn)
		for row in rows:
			cvId = str(row[0])
			scopusId = str(row[1])
			sector = row[2]
			mapping[cvId] = scopusId

res = AutoVivification()
with open("giudizi_v2.csv", mode="r") as csvfile:
	reader = csv.DictReader(csvfile, delimiter="\t")
	for row in reader:
		quad = row["QUADRIMESTRE"]
		fascia = row["FASCIA"]
		settore = row["SETTORE"]
		cvId = row["CVID"]
		i1 = row["I1"]
		i2 = row["I2"]
		i3 = row["I3"]
		i4 = row["I4"]
		i5 = row["I5"]
		i6 = row["I6"]
		i7 = row["I7"]
		i8 = row["I8"]
		i9 = row["I9"]
		i10 = row["I10"]
		ss = row["SuperaSoglie"]
		scopusId = mapping[cvId]
		
		copia = copy.deepcopy(row)
		copia["scopusId"] = scopusId
		res[scopusId][settore][fascia][int(quad)] = copia

res2 = copy.deepcopy(res)
for scopusId in res:
	for settore in res[scopusId]:
		for fascia in res[scopusId][settore]:
			if len(res[scopusId][settore][fascia].keys()) > 1:
				quads = res[scopusId][settore][fascia].keys()
				print (res2[scopusId][settore][fascia].keys())
				ultimo = max(quads)
				for quad in quads:
					if quad != ultimo:
						res2[scopusId][settore][fascia].pop(quad)
				print (res2[scopusId][settore][fascia].keys())


resList = list()
for scopusId in res2:
	temp = list()
	temp.append(scopusId)
	for settore in ["01-B1","09-H1","13-D1","13-D2","13-D3"]:
		if settore not in res2[scopusId]:
			temp += ([""] * 22)
			continue
		for fascia in ["1","2"]:
			if fascia not in res2[scopusId][settore]:
				temp += ([""] * 11)
				continue
			for quad in [1, 2, 3, 4, 5]:
				if quad not in res2[scopusId][settore][fascia]:
					continue
				values = list()
				for chiaveColonna in ["I1","I2","I3","I4","I5","I6","I7","I8","I9","I10","SuperaSoglie"]:
					values.append(res2[scopusId][settore][fascia][quad][chiaveColonna])
				temp += values
	resList.append(temp)

textOutput = "SCOPUSID\tT1_01-B1_1\tT2_01-B1_1\tT3_01-B1_1\tT4_01-B1_1\tT5_01-B1_1\tT6_01-B1_1\tT7_01-B1_1\tT8_01-B1_1\tT9_01-B1_1\tT10_01-B1_1\tSOGLIE_01-B1_1\tT1_01-B1_2\tT2_01-B1_2\tT3_01-B1_2\tT4_01-B1_2\tT5_01-B1_2\tT6_01-B1_2\tT7_01-B1_2\tT8_01-B1_2\tT9_01-B1_2\tT10_01-B1_2\tSOGLIE_01-B1_2\tT1_09-H1_1\tT2_09-H1_1\tT3_09-H1_1\tT4_09-H1_1\tT5_09-H1_1\tT6_09-H1_1\tT7_09-H1_1\tT8_09-H1_1\tT9_09-H1_1\tT10_09-H1_1\tSOGLIE_09-H1_1\tT1_09-H1_2\tT2_09-H1_2\tT3_09-H1_2\tT4_09-H1_2\tT5_09-H1_2\tT6_09-H1_2\tT7_09-H1_2\tT8_09-H1_2\tT9_09-H1_2\tT10_09-H1_2\tSOGLIE_09-H1_2\tT1_13-D1_1\tT2_13-D1_1\tT3_13-D1_1\tT4_13-D1_1\tT5_13-D1_1\tT6_13-D1_1\tT7_13-D1_1\tT8_13-D1_1\tT9_13-D1_1\tT10_13-D1_1\tSOGLIE_13-D1_1\tT1_13-D1_2\tT2_13-D1_2\tT3_13-D1_2\tT4_13-D1_2\tT5_13-D1_2\tT6_13-D1_2\tT7_13-D1_2\tT8_13-D1_2\tT9_13-D1_2\tT10_13-D1_2\tSOGLIE_13-D1_2\tT1_13-D2_1\tT2_13-D2_1\tT3_13-D2_1\tT4_13-D2_1\tT5_13-D2_1\tT6_13-D2_1\tT7_13-D2_1\tT8_13-D2_1\tT9_13-D2_1\tT10_13-D2_1\tSOGLIE_13-D2_1\tT1_13-D2_2\tT2_13-D2_2\tT3_13-D2_2\tT4_13-D2_2\tT5_13-D2_2\tT6_13-D2_2\tT7_13-D2_2\tT8_13-D2_2\tT9_13-D2_2\tT10_13-D2_2\tSOGLIE_13-D2_2\tT1_13-D3_1\tT2_13-D3_1\tT3_13-D3_1\tT4_13-D3_1\tT5_13-D3_1\tT6_13-D3_1\tT7_13-D3_1\tT8_13-D3_1\tT9_13-D3_1\tT10_13-D3_1\tSOGLIE_13-D3_1\tT1_13-D3_2\tT2_13-D3_2\tT3_13-D3_2\tT4_13-D3_2\tT5_13-D3_2\tT6_13-D3_2\tT7_13-D3_2\tT8_13-D3_2\tT9_13-D3_2\tT10_13-D3_2\tSOGLIE_13-D3_2\n"

for row in resList:
	textOutput += "\t".join(row) + "\n"
	
with open("giudizi_v3.tsv", 'w') as f:
	f.write(textOutput)
