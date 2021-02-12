#!/usr/bin/python3.5
# -*- coding: utf-8 -*-
# Author: Antoine "0x010C" Lamielle
# Date: 20 July 2018
# License: GNU GPL v3

import argparse
import hashlib
import json
import os
import sys
import threading
import time
import urllib.parse
import zipfile

import requests

# Parameters
base_url = 'http://commons.wikimedia.org/wiki/'
sparql_url = 'https://query.wikidata.org/sparql'
filenames = []
directory = ''
nb_threads = 4
output = './out.zip'
keep_files = False
force_download = False
no_zip = False
zip_file = None
nb_total_files = -1
file_format = ''

# Threading vars
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
					print('\b' * 100 + "[{0}/{1}] Download".format(nb_total_files - len(filenames), nb_total_files), end="")
					sys.stdout.flush()
				else:
					break

			get_file(fileurl, filename)

	def stop(self):
		self.__stop__ = True


def commons_file_url(filename: str, file_format: str = None, width: int = 0) -> str:
	# Returns the direct URL of a file on Wikimedia Commons.
	# Per https://frama.link/commons_path

	hashed_filename = hashlib.md5(filename.encode('utf-8')).hexdigest()

	base_url = "https://upload.wikimedia.org/wikipedia/commons"

	if width != 0:
		path = "thumb/{}/{}/{}/{}px-{}".format(
			hashed_filename[:1],
			hashed_filename[:2],
			filename,
			width,
			filename)
		if filename[-4:].lower() == '.svg':
			path += ".png"
	elif file_format is not None:
		path = "transcoded/{}/{}/{}/{}.{}".format(
			hashed_filename[:1],
			hashed_filename[:2],
			filename,
			filename,
			file_format)
	else:
		path = "{}/{}/{}".format(
			hashed_filename[:1],
			hashed_filename[:2],
			filename)

	return "{}/{}".format(base_url, path)


def get_file(fileurl, filename) -> None:
	global zip_file, base_url, file_format
	path = filename.rsplit('/', 1)[0] + '/'
	filename = filename.rsplit('/', 1)[1]

	if file_format == '':
		file_format = None
	else:
		filename = '.'.join(filename.split('.')[:-1]) + '.' + file_format
	url = commons_file_url(fileurl.replace(' ', '_'), file_format)

	if os.path.isfile(directory + path + filename) and not no_zip and not force_download:
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

	if not no_zip:
		with zip_lock:
			zip_file.writestr(path + filename, file_content)

	if keep_files:
		if directory + path == '':
			path = './'
		os.makedirs(directory + path, exist_ok=True)
		with open(directory + path + filename, 'wb') as out_file:
			out_file.write(file_content)

	del response


def get_all_files() -> None:
	"""
	Starts downloading the files.
	"""
	global zip_file, nb_total_files

	if not no_zip:
		zip_file = zipfile.ZipFile(output, "w")

	nb_total_files = len(filenames)

	threads = []
	for i in range(0, nb_threads):
		threads.append(FilesDownloader())
		threads[i].start()

	try:
		# Wait for each of the threads to finish working
		for i in range(0, nb_threads):
			threads[i].join()
	except KeyboardInterrupt:
		print("STOPPING")
		sys.stdout.flush()
		if not no_zip:
			zip_file.close()
		for i in range(0, nb_threads):
			threads[i].stop()
			threads[i].join()
	if not no_zip:
		zip_file.close()
	print('')


def get_params() -> None:
	"""
	Parses the args of the command line call.
	"""
	global base_url, sparql_url, directory, nb_threads, output, keep_files, force_download, no_zip, file_format, filenames

	# Declare the command-line arguments
	parser = argparse.ArgumentParser(description='Download a set of files from a MediaWiki instance (like Wikimedia Commons).')
	parser.add_argument('--url', help='Article path of the wiki to download files of.', default=base_url)
	parser.add_argument('--sparqlurl', help='Url of the SPARQL endpoint to use, if --sparql is used.', default=sparql_url)
	parser.add_argument('--directory', help='Folder in which to put downloaded files.', default=directory)
	parser.add_argument('--keep', help='Keep files unzipped in the directory', action='store_true', default=keep_files)
	parser.add_argument('--threads', help='Number of paralel download allowed.', type=int, default=nb_threads)
	parser.add_argument('--output', help='Output file.', default=output)
	parser.add_argument('--forcedownload', help='Download files even if they are already present locally.', action='store_true', default=force_download)
	parser.add_argument('--nozip', help='Do not zip files once downloaded.', action='store_true', default=no_zip)
	parser.add_argument('--fileformat', help='Force a specific file format.', default=file_format)
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
	force_download = args.forcedownload
	no_zip = args.nozip
	file_format = args.fileformat

	if args.category is not None:
		# TODO
		print('Coming soon')
	elif args.sparql is not None:
		response = requests.post(sparql_url, data={
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
						filenames += [(fileurl, filename)]


# Actual script execution
get_params()
get_all_files()
