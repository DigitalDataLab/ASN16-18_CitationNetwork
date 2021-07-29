# ASN16-18_CitationNetwork

Repository containing code for the paper titled "Are citation networks relevant to explain academic promotions? An empirical analysis of the Italian national scientific qualification".

## Data

The folder `data/` containts the input data:
- `candidates.tsv`: contains information about the publications of the ASN 2016-18 candidates in the RFs 01/B1, 09/H1, 13/D1, 13/D2 and 13/D3
- `soglie_2016_bibliometrici.tsv` and `soglie_2016_non-bibliometrici.tsv`: the files contain the thresholds for the three bibliometric indicators used in the first phase of the ASN for Citation-based Disciplines (CDs) and Non-citation-based Disciplines (NDs), respectively
- `titoli_2016`: the official description (in Italian) of the ten accomplishments considered by the ASN committees in their assessments

## Code

The folder `script/` contains the Python scripts to perform the data processing workflow. The process is organized in seven steps (i.e. 01-07) which must be runned in sequence. A valid Scoups API key should be specified in the `apikeys.py` file in order to execute the process. The files `mylib.py` and `conf.py` contain library functions and configuration information.

The results of the computations are stored in the `data/output` folder.
