import sqlite3
from sqlite3 import Error

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
 
 
def main():
	sql_create_authorScopus_table = """CREATE TABLE IF NOT EXISTS authorScopus (
									id integer PRIMARY KEY,
									givenname text,
									surname text,
									initials text,
									orcid text
								);"""

	sql_create_curriculum_table = """ CREATE TABLE IF NOT EXISTS curriculum (
										id integer PRIMARY KEY,
										authorId integer,
										annoAsn text NOT NULL,
										settore text NOT NULL,
										ssd text,
										quadrimestre integer NOT NULL,
										fascia integer NOT NULL,
										orcid text,
										cognome text,
										nome text,
										bibl integer,
										I1 integer NOT NULL,
										I2 integer NOT NULL,
										I3 integer NOT NULL,
										idSoglia integer NOT NULL,
										esito text NOT NULL,
										FOREIGN KEY (idSoglia) REFERENCES sogliaAsn(id)
										FOREIGN KEY (authorId) REFERENCES authorScopus(id)
									); """

 
	sql_create_cercauniversita_table = """CREATE TABLE IF NOT EXISTS cercauniversita (
									id string PRIMARY KEY,
									authorId integer,
									anno text NOT NULL,
									settore text NOT NULL,
									ssd text,
									fascia integer NOT NULL,
									orcid text,
									cognome text,
									nome text,
									genere text,
									ateneo text,
									facolta text,
									strutturaAfferenza,
									FOREIGN KEY (authorId) REFERENCES authorScopus(id)
								);"""

	'''
	sql_create_esitoAsn_table = """ CREATE TABLE IF NOT EXISTS esitoAsn (
										*idCv integer PRIMARY KEY,
										*annoAsn text NOT NULL,
										*settore text NOT NULL,
										*descrSettore text NOT NULL,
										*ssd text,
										*quadrimestre integer NOT NULL,
										*fascia integer NOT NULL,
										I1 integer NOT NULL,
										I2 integer NOT NULL,
										I3 integer NOT NULL,
										idSoglia integer NOT NULL,
										esito text NOT NULL,
										*FOREIGN KEY (idCv) REFERENCES curriculum(id),
										FOREIGN KEY (idSoglia) REFERENCES sogliaAsn(id)
									); """
	'''
	
	sql_create_sogliaAsn_table = """ CREATE TABLE IF NOT EXISTS sogliaAsn (
										id integer PRIMARY KEY,
										annoAsn text NOT NULL,
										settore text NOT NULL,
										descrSettore string,
										ssd text,
										fascia integer NOT NULL,
										S1 integer,
										S2 integer,
										S3 integer,
										descrS1 string NOT NULL,
										descrS2 string NOT NULL,
										descrS3 string NOT NULL,
										bibl integer
									); """

	sql_create_wroteRelation_table = """ CREATE TABLE IF NOT EXISTS wroteRelation (
										authorId integer NOT NULL,
										eid string,
										FOREIGN KEY (eid) REFERENCES authorScopus(id),
										FOREIGN KEY (eid) REFERENCES publication(eid)
									); """

	sql_create_publication_table = """ CREATE TABLE IF NOT EXISTS publication (
										eid string PRIMARY KEY,
										doi string,
										publicationDate string,
										publicationYear string,
										title string,
										venueName string,
										aggregationType string,
										subtypeDescription string
									); """

	sql_create_citesRelation_table = """ CREATE TABLE IF NOT EXISTS citesRelation (
										eidCiting string,
										eidCited string,
										citationDate string,
										citationYear string NOT NULL
									); """

	# create a database connection
	conn = create_connection(conf.dbFilename)
 
	# create tables
	if conn is not None:
		# create authorScopus table
		create_table(conn, sql_create_authorScopus_table)
 
		# create curriculum table
		create_table(conn, sql_create_curriculum_table)

		# create cercauniversita table
		create_table(conn, sql_create_cercauniversita_table)

		#create_table(conn, sql_create_esitoAsn_table)
		create_table(conn, sql_create_sogliaAsn_table)
		create_table(conn, sql_create_wroteRelation_table)
		create_table(conn, sql_create_publication_table)
		create_table(conn, sql_create_citesRelation_table)
		
		conn.close()
	else:
		print("Error! cannot create the database connection.")
	
	
if __name__ == '__main__':
	main()
