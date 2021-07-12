#!/usr/bin/python
import argparse
from bs4 import BeautifulSoup
from concurrent.futures.thread import ThreadPoolExecutor
import hashlib
import logging
import os
import pandas as pd
import re, requests
import time, threading
import urllib3
urllib3.disable_warnings()

#----------------------------------------------------------------------------
#Program arguments and settings

#Parser, this allows user to specify arguments for the script
parser = argparse.ArgumentParser(description="Automated batch downloading documents from Investegate")
parser.add_argument('qsArticleType', help="Type of document to download Announcement/News", choices=["ann", "news"])
parser.add_argument('-t' '--time', help="Time period of documents", type=int, default="12", dest="qsSpan")
parser.add_argument('-c', '--category', help="Category of documents to download", dest="qsCategory")
parser.add_argument('-s', '--search', help="Index for searching companies name/EPIC/SEDOL", default="S1", dest="qsSearchFor", choices=["S1", "S2", "S3"])
parser.add_argument('-n', '--name', help="Search for company using specified index", dest="qsContains", type=str)
parser.add_argument('-k', '--keyword', help="Search using keyword", dest="qsKeyWord", type=str)
parser.add_argument('-w', '--wordlist', help="Search for documents by wordlist", dest="wordlist")
parser.add_argument('-r', '--threads', help="Number of threads", dest="threads", default=50)
parser.add_argument('-v', '--verbose', help="Produce verbose output", dest="verbose", action="store_true")


#Imopratnt variables
global url
url = "https://investegate.co.uk/AdvancedSearch.aspx"
conf = vars(parser.parse_args())
verbose = conf.pop("verbose")
level = logging.INFO
if verbose == True:
	level = logging.DEBUG
log_console_format = "[%(threadName)s]: %(message)s"
logging.basicConfig(level=level, format='%(levelname)s: %(message)s')


#Extracts all necessary information from user arguments
def extractConf(conf):
	if conf["qsArticleType"] == "news":
		conf["qsNewsCategory"] = conf.pop("qsCategory")
	if conf["qsContains"] != None:
		conf["qsContains"] = [s.strip() for s in re.sub(" ", "", conf["qsContains"]).split(",")]
	df_path = conf.pop("wordlist")
	threads = conf.pop("threads")
	if df_path != None and os.path.isfile(df_path):
		wordlist = pd.read_csv(df_path)
	else:
		index = range(len(conf["qsContains"]))
		wordlist = pd.DataFrame({conf["qsSearchFor"]:conf.pop("qsContains")}, index=index)
	conf = {key:val for key, val in conf.items() if val != None}
	return conf, wordlist, threads



#--------------------------------------------------------------------------
#Downloading and processing documents

def scrapeInfo(conf, word, proxy):
	finished = False
	allFinish = False
	conf = conf.copy()
	conf["qsContains"], conf["pno"] = word, 1
	all_dict = {}
	while not finished:
		text = makeRequest(conf, proxy=proxy)
		company_dict, num_docs = filterResponse(text)
		all_dict.update(company_dict)
		#num_docs = len(company_dict)
		if num_docs == 0:
			finished = True
		downloadDocs(company_dict, proxy=proxy)
		conf["pno"] += 1
	return all_dict

#Makes a request to the url with the specified conf, returns a list of documents to be downloaded
def makeRequest(conf, proxy=None):
	a = time.time()
	#Makes request
	r = requests.get(url, params=conf, proxies=proxy, verify=False)
	logging.debug("[*] Time taken - {0}".format(time.time() - a))
	return r.text

#Give the html code of the website, extract the document info such as Name and Link to text
def filterResponse(text):
	company_dict = {}
	num_docs = 0
	for doc in getDocIndex(text):
		num_docs += 1
		ctext = text[doc:]
		code = ctext.split("&amp")[0]
		company_name = re.sub("\s\(.*\)", "", re.split(">|<", ctext)[3])
		extension = re.search("href=\".*\"", ctext).group().split("href=\"")[1].strip("\"")
		link = url + extension
		title = re.search("\/\">.*<\/a>", ctext).group().strip("/\"").strip("</a>")
		timestamp = link.split('/')[-2][:8]
		#TEMPORARY
		filter_list = ["2009", "2008", "2010", "2011"]
		if filterByDate(timestamp, filter_list):
			if code not in company_dict.keys():
				company_dict[code] = [{"companyName":company_name, "docLink":link, "title":title, "timestamp":timestamp}]
			else:
				company_dict[code].append({"companyName":company_name, "docLink":link, "title":title, "timestamp":timestamp})
	return company_dict, num_docs

def getDocIndex(text):
	return (match.end(0) for match in re.finditer("CompData\.aspx\?code=", text))

#Given document info, download and save the document text
def downloadDocs(company_dict, proxy=None):
	limit, ID = 1, 1#50, 1
	with ThreadPoolExecutor(max_workers=limit) as doc:
		for company, code in zip(list(company_dict.values()), list(company_dict.keys())):
			for entry in company:
				docThread = DocThread(ID)
				doc.submit(docThread.downloadDoc, entry, code)
		
#Download a single document
def downloadSingle(filename, entry):
	if os.path.isfile(filename):
		logging.debug("Skipped")
		return
	link = entry["docLink"]
	html = requests.get(link, proxies=proxy).text
	doc = BeautifulSoup(html, "lxml").text
	doc = re.sub("\n+", "\n", doc)
	#Filter all unecessary information away
	index = re.search("[0-3][0-9] (?:January|February|March|April|May|August|June|July|September|October|November|December), (?:19|20)\d\d", doc).end(0)
	endex = re.search("This information is provided by RNS", doc).start(0)
	filtered_doc = doc[index:endex]
	with open(filename, "wb") as writer:
		writer.write(filtered_doc.encode('utf8'))

#gets documentName for us to write to
def getDocName(entry):
	return re.sub('/', '_', entry["docLink"].split(url)[1])

class DownloadThread():
	def __init__(self, threadID):
		self.ID = threadID
		self._lock = threading.Lock()
	def operate(self, word, conf, proxy):
		logging.info("[+] Starting Download Thread {0}".format(self.ID))
		logging.debug("[*] Thread {0} accessing word {1}".format(self.ID, word))
		
		self.conf = conf.copy()
		company_dict = scrapeInfo(self.conf, word, proxy)
		logging.info("[+] Ending Download Thread {0}".format(self.ID))

class DocThread():
	def __init__(self, childID):
		threading.Thread.__init__(self)
		self.ID = childID
		self._lock = threading.Lock()
	def downloadDoc(self, entry, code):
		elements = [entry for key, entry in entry.items() if key != "docLink"]
		filename = os.path.join("docs", code+"_"+"_".join(elements)+".txt")
		with self._lock:
			logging.debug("[*] Doc Thread {0} accessing file - {0}".format(filename))
		downloadSingle(filename, entry)


#WARNING!
#OBSELETE SECTION START
#---------------------------------------------------------------------
#CSV dataset file operations
def saveCSV(company_dict):
	columns = ["code", "companyName", "title", "docName", "timestamp"]
	df = pd.DataFrame(columns=columns)
	for code, company in zip(company_dict.keys(), company_dict.values()):
		for entry in company:
			docName = getDocName(entry)
			df = df.append({"code":code, "companyName":entry["companyName"], "title":entry["title"], "docName":docName, "timestamp":entry["timestamp"]}, ignore_index=True)
	return df
def writeCSV(df1):
	df1["timestamp"] = pd.to_datetime(df1["timestamp"], format="%Y%m%d")
	df2 = pd.read_csv("df.csv")
	df_new = pd.concat([df1,df2]).drop_duplicates().reset_index(drop=True)
	df_new.to_csv("df.csv", index=False)

#OBSELETE SECTION FINISH

#-------------------------------------------------------------------------------
#Developer stuff
def filterByDate(timestamp, filter_list):
	if str(timestamp[0:4]) in filter_list:
		return True
	else:
		return False
#-------------------------------------------------------------------------------
#Main programs of the file
def main(conf, threads, wordlist=None, proxy=None):
	startThread(wordlist, threads, conf)
	logging.info("[+] Finished downloading {0} docs in {1} secs".format(len(os.listdir("docs/")), time.time() - time_a))
def startThread(wordlist, threads, conf):
	ID = 1
	with ThreadPoolExecutor(max_workers=int(threads)) as executor:
		for word in wordlist[conf["qsSearchFor"]]:
			thread = DownloadThread(ID)
			executor.submit(thread.operate, word, conf, proxy)
			ID += 1

conf, wordlist, threads = extractConf(conf)
logging.info("[+] Using {0} threads".format(threads))
proxy = None#{"http":"http://127.0.0.1:8080", "https":"http://127.0.0.1:8080"}
time_a = time.time()
main(conf, threads, wordlist=wordlist, proxy=proxy)

