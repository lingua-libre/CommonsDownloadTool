#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
#Autor: Antoine "0x010C" Lamielle
#Date: 20 July 2018
#License: GNU GPL v3

from __future__ import print_function
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

#Parameters
base_url = 'http://commons.wikimedia.org/wiki/'
sparql_url = 'https://query.wikidata.org/sparql'
filenames = []
directory = ''
nb_threads = 4
output = './out.zip'
keep_files = False
zip_file = None
split = None
split_level = 3
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
					filename = filenames.pop()
					print('\b'*(100)+"[{0}/{1}] Download".format(nb_total_files-len(filenames), nb_total_files), end="")
					sys.stdout.flush()
				else:
					break

			get_file(filename)

	def stop(self):
		self.__stop__ = True

def get_file(filename):
	global zip_file, base_url, split, split_level
	url = base_url + 'Special:FilePath/' + filename
	path = directory
	if split != None:
		path += filename.replace(split, '/', split_level).rsplit('/',1)[0] + '/'

	if os.path.isfile(path + filename):
		with zip_lock:
			zip_file.write(path + filename)
		return

	response = requests.get(url, stream=True)

	file_content = response.content

	with zip_lock:
		zip_file.writestr(path + filename, file_content)

	if keep_files:
		if path == '':
			path = './'
		os.makedirs(path, exist_ok=True)
		with open(path + filename, 'wb') as out_file:
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
	global base_url, sparql_url, directory, nb_threads, output, keep_files, filenames, split, split_level


	# Declare the command-line arguments
	parser = argparse.ArgumentParser(description='Download a set of files from a MediaWiki instance (like Wikimedia Commons).')
	parser.add_argument('--url', help='Article path of the wiki to download files of.', default=base_url)
	parser.add_argument('--sparqlurl', help='Url of the SPARQL endpoint to use, if --sparql is used.', default=sparql_url)
	parser.add_argument('--directory', help='Folder in which to put downloaded files.', default=directory)
	parser.add_argument('--keep', help='Keep files unzipped in the directory', action='store_true', default=False)
	parser.add_argument('--threads', help='Number of paralel download allowed.', type=int, default=nb_threads)
	parser.add_argument('--output', help='Output file.', default=output)
	parser.add_argument('--split', help='Separate files in subdirectories by spliting according to the given char')
	parser.add_argument('--splitlevel', help='Set the maximum level of subdirectories if --split is set', type=int, default=split_level)
	sourcegroup = parser.add_mutually_exclusive_group(required=True)
	sourcegroup.add_argument('--category', help='Use a category to generate the list of files to download')
	sourcegroup.add_argument('--sparql', help='Use a sparql request to generate the list of files to download')

	# Parse the command-line arguments
	args = parser.parse_args()

	base_url = args.url
	directory = args.directory
	nb_threads = args.threads
	output = args.output
	keep_files = args.keep
	split = args.split
	split_level = args.splitlevel
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
			for cell in line:
				if line[cell]['type'] == 'uri':
					if line[cell]['value'].startswith(base_url):
						filenames += [ line[cell]['value'].split('/')[-1] ]


get_params()
get_all_files()
