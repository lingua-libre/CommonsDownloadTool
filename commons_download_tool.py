#!/usr/bin/python3.5
# -*- coding: utf-8 -*-
#Autor: Antoine "0x010C" Lamielle
#Date: 20 July 2018
#License: GNU GPL v3

import sys
import os
import requests
import shutil
import threading
from subprocess import call
import argparse
import hashlib
import zipfile
import io
import json
import urllib.parse
import time

#Parameters
base_url = 'http://commons.wikimedia.org/wiki/'
sparql_url = 'https://query.wikidata.org/sparql'
filenames = []
directory = ''
nb_threads = 4
output = './out.zip'
keep_files = False
zip_file = None
nb_total_files = -1

#Threading vars
filename_lock = threading.Lock()
zip_lock = threading.Lock()


class FilesDownloader(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.__stop__ = False

	def run(self):
		while not self.__stop__:
			with filename_lock:
				if len(filenames) > 0:
					(fileurl, filename) = filenames.pop()
					print('\b'*(100)+"[{0}/{1}] Download".format(nb_total_files-len(filenames), nb_total_files), end="")
					sys.stdout.flush()
				else:
					break

			get_file(fileurl, filename)

	def stop(self):
		self.__stop__ = True

def get_file(fileurl, filename):
	global zip_file, base_url
	url = base_url + 'Special:FilePath/' + fileurl
	path = filename.rsplit('/',1)[0] + '/'
	filename = filename.rsplit('/',1)[1]

	if os.path.isfile(directory + path + filename):
		with zip_lock:
			zip_file.write(directory + path + filename, arcname=path + filename)
		return

	while True:
		response = requests.get(url, stream=True)
		if response.status_code == 200:
			break
		elif response.status_code == 500 or response.status_code == 429:
			print('\nstatus_code: '+str(response.status_code))
			time.sleep(10)
		else:
			print('\nstatus_code: '+str(response.status_code))
			del response
			return

	file_content = response.content

	with zip_lock:
		zip_file.writestr(path + filename, file_content)

	if keep_files:
		if directory + path == '':
			path = './'
		os.makedirs(directory + path, exist_ok=True)
		with open(directory + path + filename, 'wb') as out_file:
			out_file.write(file_content)

	del response

def get_all_files():
	global zip_file, nb_total_files
	zip_file = zipfile.ZipFile(output, "w")
	nb_total_files = len(filenames)

	threads = []
	for i in range(0,nb_threads):
		threads.append(FilesDownloader())
		threads[i].start()

	try:
		for i in range(0,nb_threads):
			threads[i].join()
	except KeyboardInterrupt:
		print("STOPPING")
		sys.stdout.flush()
		zip_file.close()
		for i in range(0,nb_threads):
			threads[i].stop()
			threads[i].join()
	zip_file.close()
	print('')



def get_params():
	global base_url, sparql_url, directory, nb_threads, output, keep_files, filenames


	# Declare the command-line arguments
	parser = argparse.ArgumentParser(description='Download a set of files from a MediaWiki instance (like Wikimedia Commons).')
	parser.add_argument('--url', help='Article path of the wiki to download files of.', default=base_url)
	parser.add_argument('--sparqlurl', help='Url of the SPARQL endpoint to use, if --sparql is used.', default=sparql_url)
	parser.add_argument('--directory', help='Folder in which to put downloaded files.', default=directory)
	parser.add_argument('--keep', help='Keep files unzipped in the directory', action='store_true', default=False)
	parser.add_argument('--threads', help='Number of paralel download allowed.', type=int, default=nb_threads)
	parser.add_argument('--output', help='Output file.', default=output)
	sourcegroup = parser.add_mutually_exclusive_group(required=True)
	sourcegroup.add_argument('--category', help='Use a category to generate the list of files to download')
	sourcegroup.add_argument('--sparql', help='Use a sparql request to generate the list of files to download; must contain a ?file field and can have an optional ?filename field')

	# Parse the command-line arguments
	args = parser.parse_args()

	base_url = args.url
	directory = args.directory
	nb_threads = args.threads
	output = args.output
	keep_files = args.keep
	sparql_url = args.sparqlurl


	if args.category != None:
		#TODO
		print('Comming soon')
	elif args.sparql != None:
		response = requests.post(sparql_url, data = {
			'format': 'json',
			'query': args.sparql
		})
		response = json.loads(response.text)['results']['bindings']
		for line in response:
			if 'file' in line:
				if line['file']['type'] == 'uri':
					if line['file']['value'].startswith(base_url):
						fileurl = urllib.parse.unquote(line['file']['value'].split('/')[-1])
						filename = fileurl
						if 'filename' in line:
							filename = urllib.parse.unquote(line['filename']['value'])
						filenames += [ (fileurl, filename) ]


get_params()
get_all_files()
