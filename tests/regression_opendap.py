import subprocess
import netCDF4
from timeit import default_timer as timer
import numpy as np
import os
import shutil
try:
	from urllib.request import urlopen
except ImportError:
	from urllib2 import urlopen
from contextlib import closing

"""
Regressietest voor OPeNDAP

 ---- Doel ----
 Het doel van deze regressietest is nagaan of requests via OPeNDAP correct resultaten opleveren
 en of de performance op peil blijft.

 ---- Methode ----
 OPeNDAP functioneert correct als dit bij een query hetzelfde resultaat als een lokale query (i.e. via ncdump)
 Dit betekent dat zowel de header hetzelfde moet blijven alsmede dat de data hetzelfde is.
 In dit script wordt dit getest door een request te doen naar een dataset via OPeNDAP en tegelijkertijd een 
 granule uit dezelfde dataset te parsen (beide d.m.v. de python-netCDF4 library). Deze granule moet volledig 
 bevat zijn in het OPeNDAP resultaat. Ook moet in de header van de granule dezelfde variabelen en dimensies 
 voorkomen als in de OPeNDAP header.

 Om de performance te meten is er een vooraf bepaalde referentietijd voor die dataset op dat domein hoe lang 
 het request er over mag doen. Indien de performance <95% is, zal er een waarschuwing worden gegeven.
 Indien de performance <90% is, zal het programma crashen met een fatale fout omdat dit hoogst waarschijnlijk een
 regressie is.

 ---- Risico's en overwegingen ----
 - De performance wordt momenteel getest op een vCloud omgeving. Deze heeft een onvoorspelbare performance en
   kan regelmatig iets als een regressie aanduiden wat eigenlijk een vCloud probleem is. 
 - Headers worden doorgaans opgehaald `ncdump`. Dit kan in python aangeroepen worden d.m.v. een shell command
   maar dit geeft een string als output, wat vergelijken op correctheid bemoeilijkt. Om dit op te lossen wordt
   de python-netCDF4 library gebruikt die -- als het goed is -- consistente output zou moeten geven met `ncdump`.
"""

def get_url(address='', dataset_name=''):
	_url = ''
	if address and dataset_name:
		_url = 'https://guest:guest@{0}/wms/cgi-bin/wms.cgi/opendap/{1}/'.format(address, dataset_name)
	else:
		raise ValueError('An address and dataset name are required')
	return _url
def read_remote_data(address='', dataset_name='', reference_time=-1, yell=True):
	url = get_url(address=address, dataset_name=dataset_name)
	start = timer()
	remote_data = netCDF4.Dataset(url)
	time_taken = timer() - start

	if yell:
		print('Done. Took {0} s (as opposed of {1} s as reference)'.format(round(time_taken, 4), reference_time))

	return time_taken, remote_data


def fetch_and_test_performance(wms_address='', dataset_name='', reference_time=-1, ratio_warn=0.95, ratio_error=0.9):
	print('Reading remote file from resource...')
	time_opendap, dataset_opendap = read_remote_data(address=wms_address, dataset_name=dataset_name, reference_time=reference_time)

	perf_ratio = reference_time / time_opendap
	assert perf_ratio > ratio_error, 'Newer source took significantly longer than reference measurement. Regression?'
		
	if perf_ratio < ratio_warn:
		print('WARNING: OPeNDAP took more than the usual amount of time to fetch data. Regression?')

	return dataset_opendap


def datasame(dataset_local, dataset_opendap, time):
	timedim = dataset_opendap.variables['time']
	date = netCDF4.num2date(time, timedim.units)
	if 'calendar' in timedim.ncattrs():
		calendar = timedim.getncattr('calendar')
		if calendar == 'none':
			calendar = 'standard'
		date_idx = netCDF4.date2index(date, timedim, calendar=calendar)
	else:
		date_idx = netCDF4.date2index(date, timedim)

	for var in dataset_local.variables.keys():
		if var == 'time':
			continue

		if dataset_local.variables[var].shape:
			if dataset_opendap.variables[var].dimensions[0] == 'time':
				assert np.array_equal(dataset_local.variables[var][0, :], dataset_opendap.variables[var][date_idx, :])
			else:
				assert np.array_equal(dataset_local.variables[var][:], dataset_opendap.variables[var][:])
	return True

def test_correctness(dataset_local, dataset_opendap, time):
	assert sorted(dataset_local.variables.keys()) == sorted(dataset_opendap.variables.keys())
	assert datasame(dataset_local, dataset_opendap, time)

def test_opendap(local_file_name='', wms_address='', dataset_name='', reference_time=-1):
	"""
		Test of OPeNDAP
	"""
	print('Reading local file...')
	start = timer()
	dataset_local = netCDF4.Dataset(local_file_name)
	local_time = timer() - start
	print('Done. Took {} s'.format(round(local_time, 4)))

	ncdf_time = dataset_local.variables['time'][0]

	dataset_opendap = fetch_and_test_performance(wms_address=wms_address, dataset_name=dataset_name, reference_time=reference_time)
	test_correctness(dataset_local, dataset_opendap, ncdf_time)

def test_ncdump_opendap_header(local_file_name='', wms_address='', dataset_name=''):
	url = get_url(address=wms_address, dataset_name=dataset_name)
	remote = netCDF4.Dataset(url)
	local = netCDF4.Dataset(local_file_name)

	assert(sorted(remote.variables.keys()) == sorted(local.variables.keys()))
	assert(set(local.dimensions.keys()).issubset(remote.dimensions.keys()))

	# Should be done like this, but returns a string so poor comparison 
	# statistical approach kinda works
	# remote_header = subprocess.check_output(['ncdump', '-h', url])
	# local_header = subprocess.check_output(['ncdump', '-h', local_file_name])
	# correctcount = 0
	# remote_lines = sorted(remote_header.splitlines())
	# for i in sorted(local_header.splitlines()):
	# 	if i in remote_lines:
	# 		correctcount = correctcount + 1

	# assert correctcount / float(len(remote_lines)) > 0.85

def check_files_present():
	files = ['INTER_OPER_R___RD1NRT__L3__20170409T080000_20170410T080000_0001.nc', 'ESACCI-OZONE-L4-NP-MERGED-KNMI-20081220-fv0003.nc', 'SEVIR_OPER_R___MSGCPP__L2__20170401T000000_20170402T000000_0002.nc']
	missing_files = [file for file in files if not(os.path.isfile(file))]
	if len(missing_files) > 0:
		print('Not all files are present. Do you want to download the missing files?')
		try:
			choice = raw_input().lower()
		except NameError:
			choice = input().lower()
		download = choice[0] == 'y'
	else:
		download = False
	return download, missing_files

def download_missing_files(which_files, wms_address):
	urls = {
		'SEVIR_OPER_R___MSGCPP__L2__20170401T000000_20170402T000000_0002.nc': 'ftp://{0}/download/Dataset with very large files/2/0002/2017/04/01/SEVIR_OPER_R___MSGCPP__L2__20170401T000000_20170402T000000_0002.nc'.format(wms_address),
		'INTER_OPER_R___RD1NRT__L3__20170409T080000_20170410T080000_0001.nc': 'ftp://{0}/download/Many small files/1/0001/2017/04/09/INTER_OPER_R___RD1NRT__L3__20170409T080000_20170410T080000_0001.nc'.format(wms_address),
		'ESACCI-OZONE-L4-NP-MERGED-KNMI-20081220-fv0003.nc': 'ftp://{0}/download/Many large files/1/noversion/2008/12/14/ESACCI-OZONE-L4-NP-MERGED-KNMI-20081220-fv0003.nc'.format(wms_address)
	}
	for file in which_files:
		url = urls[file]
		print('Downloading {0}'.format(file))
		with closing(urlopen(url)) as r:
			with open(file, 'wb') as f:
				shutil.copyfileobj(r, f)
		print('Done')

if __name__ == '__main__':
	WMS_ADDRESS = '145.23.218.149'
	download, which_files = check_files_present()
	if download:
		download_missing_files(which_files=which_files, wms_address=WMS_ADDRESS)
	# ---- Dataset with many small files ---- #
	LOCAL_FILE_NAME = 'INTER_OPER_R___RD1NRT__L3__20170409T080000_20170410T080000_0001.nc'
	DATASET_NAME='Many%20small%20files_1'
	REF_TIME = 1.2 # second

	test_ncdump_opendap_header(local_file_name=LOCAL_FILE_NAME, wms_address=WMS_ADDRESS, dataset_name=DATASET_NAME)
	test_opendap(local_file_name=LOCAL_FILE_NAME, wms_address=WMS_ADDRESS, dataset_name=DATASET_NAME, reference_time=REF_TIME)

	# ---- Dataset with many large files ---- #
	# LOCAL_FILE_NAME = 'ESACCI-OZONE-L4-NP-MERGED-KNMI-20081220-fv0003.nc'
	# DATASET_NAME='Many%20large%20files_1'
	# REF_TIME = 1.5 # second
	# test_ncdump_opendap_header(local_file_name=LOCAL_FILE_NAME, wms_address=WMS_ADDRESS, dataset_name=DATASET_NAME)
	# test_opendap(local_file_name=LOCAL_FILE_NAME, wms_address=WMS_ADDRESS, dataset_name=DATASET_NAME, reference_time=REF_TIME)

	# ---- Dataset with few huge files ---- #
	# DATASET_NAME='Dataset%20with%20very%20large%20files_2'
	# LOCAL_FILE_NAME = 'SEVIR_OPER_R___MSGCPP__L2__20170401T000000_20170402T000000_0002.nc'
	# REF_TIME = 1 # second

	# test_ncdump_opendap_header(local_file_name=LOCAL_FILE_NAME, wms_address=WMS_ADDRESS, dataset_name=DATASET_NAME)
	# test_opendap(local_file_name=LOCAL_FILE_NAME, wms_address=WMS_ADDRESS, dataset_name=DATASET_NAME, reference_time=REF_TIME)

	print('DONE! All clear.')
