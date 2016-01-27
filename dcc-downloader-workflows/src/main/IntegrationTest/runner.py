#!/usr/bin/python
#
# Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
#
# This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
# SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
# TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from org.apache.pig.scripting import *
import tarfile
import shutil
import urllib2
import time
try: from com.xhaus.jyson import JysonCodec as json
except ImportError: import json
import os
import os.path
import sys, getopt

# downloadEndPointUrl = 'http://***REMOVED***:8888/api/download/'
downloadEndPointUrl = 'http://***REMOVED***:8888/api/v1/download/'
validationRootDir = '/tmp/dd-validation/'

# Donor Id field position
donorIDPos = {
        'donor' : '$0',
        'sample' : '$5',
        'ssm_open' : '$1',
        'ssm_controlled' : '$1',
        'sgv_controlled' : '$0',
        'cnsm' : '$0',
        'stsm' : '$0',
        'exp' : '$0',
        'exp_seq' : '$0',
        'exp_array' : '$0',
        'mirna' : '$0',
        'mirna_seq' : '$0',
        'pexp' : '$0',
        'jcn' : '$0',
        'meth' : '$0',
        'meth_array' : '$0',
        'meth_seq' : '$0'
        }

# Project code field position
projectCodePos = {
        'donor' : '$1',
        'sample' : '$1',
        'ssm_open' : '$2',
        'ssm_controlled' : '$2',
        'sgv_controlled' : '$1',
        'cnsm' : '$1',
        'stsm' : '$1',
        'exp' : '$1',
        'exp_seq' : '$1',
        'exp_array' : '$1',
        'mirna' : '$1',
        'mirna_seq' : '$1',
        'pexp' : '$1',
        'jcn' : '$1',
        'meth' : '$1',
        'meth_array' : '$1',
        'meth_seq' : '$1'
        }

type2ui = {
        'clinical' : 'clinical',
        'clinicalsample' : 'clinical',
        'ssm_open' : 'ssm',
        'ssm_controlled' : 'ssm',
        'sgv_controlled' : 'sgv',
        'cnsm' : 'cnsm',
        'stsm' : 'stsm',
        'exp' : 'exp',
        'exp_seq' : 'exp_seq',
        'exp_array' : 'exp_array',
        'mirna' : 'mirna',
        'mirna_seq' : 'mirna_seq',
        'pexp' : 'pexp',
        'jcn' : 'jcn',
        'meth' : 'meth',
        'meth_array' : 'meth_array',
        'meth_seq' : 'meth_seq'
        }

longname = {
        'clinical' : 'clinical',
        'clinicalsample' : 'clinicalsample',
        'ssm_open' : 'simple_somatic_mutation.open',
        'ssm_controlled' : 'simple_somatic_mutation.controlled',
        'sgv_controlled' : 'simple_germline_variation',
        'cnsm' : 'copy_number_somatic_mutation',
        'stsm' : 'structural_somatic_mutation',
        'exp' : 'gene_expression',
        'exp_seq' : 'exp_seq',
        'exp_array' : 'exp_array',
        'mirna' : 'mirna_expression',
        'mirna_seq' : 'mirna_seq',
        'pexp' : 'protein_expression',
        'jcn' : 'splice_variant',
        'meth' : 'methylation',
        'meth_seq' : 'meth_seq',
        'meth_array' : 'meth_array'
        }

params = {}

# submit job
def submitJob(projects, datatype):
	selections = []
	for project in projects:
		selections.append('%22' + project + '%22')
	projectQueryString = '%7B"donor":%7B"projectId":%7B"is":%5B' + ",".join(selections) + '%5D%7D%7D%7D'
	# projectQueryString = '{%22project%22:{%22_project_id%22:[' + ",".join(selections) + ']}}'
	dataTypeQueryString = '[{%22key%22:%22' + type2ui[datatype] + '%22,%22value%22:%22TSV%22}]'
	url = downloadEndPointUrl + 'submit?filters=' + projectQueryString + '&info=' + dataTypeQueryString + '&email=&downloadUrl=/downloader'
	print("Request URL: " + url)
	req = urllib2.Request(url)
	resp = urllib2.urlopen(req)
	data = json.loads(resp.read().decode('utf8'))
	resp.close()
	downloadId = data['downloadId']
	print("Download ID: " + downloadId)
	return downloadId

# check status
def wait(downloadId):
	status = {'status': 'RUNNING'}
	while status['status'] != 'SUCCEEDED':
		time.sleep(10)
		statusUrl = downloadEndPointUrl + downloadId + '/status'
		statusReq = urllib2.Request(statusUrl)
		resp = urllib2.urlopen(statusReq)
		statusData = json.loads(resp.read().decode('utf8'))
		resp.close()
		status = statusData[0]
		print("Workflow Status: " + status['status'])

# download the archive
def download(downloadId, dataType, testDir):
	dlUrl = downloadEndPointUrl + downloadId
	dlReq = urllib2.Request(dlUrl)
	dlResp = urllib2.urlopen(dlReq)
	fo = open("download/" + downloadId + ".tar", "wb")
	shutil.copyfileobj(dlResp, fo)
	fo.close()
	# untar the download
	tar = tarfile.open("download/" + downloadId + ".tar")
	tar.extract(dataType + ".tsv.gz", testDir)
	os.system("gunzip " + testDir + '/*')

# copy to HDFS
def copyToHDFS(toDir):
	os.system("HADOOP_USER_NAME=downloader hdfs dfs -rm -r " + validationRootDir + toDir)
	os.system("HADOOP_USER_NAME=downloader hdfs dfs -mkdir -p " + validationRootDir + toDir + '/download/')
	os.system("HADOOP_USER_NAME=downloader hdfs dfs -put " + toDir + "/* " + validationRootDir + toDir + '/download/')

def validate(type, projects, experiment, staticRootDir):
	output = validationRootDir + experiment
	params["DYNAMIC_FILES"] = output + '/download/*'
	params["STATIC_FILES"] = staticRootDir + '/{' + ','.join(projects) + '}/' + longname[type] + '.*'
	params["SELECTIONS"] = ','.join([r"""\'%s\'""" % p for p in projects])
	params["DATA_TYPE"] = type
	params["DONOR_ID_POS"] = donorIDPos[type]
	params["PROJECT_CODE_POS"] = projectCodePos[type]
	os.system("HADOOP_USER_NAME=downloader hdfs dfs -rm -r " + output + "/result")
	params["OUTPUT_CONTENT_MISMATCH"] = output + '/result/content'
	params["OUTPUT_HEADER_MISMATCH"] = output + '/result/header'
	Pig.set('default_parallel', '100')
	P = Pig.compileFromFile(type, "validation.pig")
	bound = P.bind(params)
	stats = bound.runSingle()
	if not stats.isSuccessful():
		for errMsg in stats.getAllErrorMessages():
			print errMsg
		raise 'failed'

def main(argv) :
	projects = ''
	type = ''
	experiment = 'unname'
	staticRootDir = ''
	try:
		opts, args = getopt.getopt(argv, "ht:n:p:i:", ["type=", "name=", "projects=", "input="])
	except getopt.GetoptError:
		print 'test.py -p <projectlist> -t <datatype> -n <experimentname>'
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print 'test.py -p <projectlist> -t <datatype> -n <experimentname>'
			sys.exit()
		elif opt in ("-n", "--name"):
		  experiment = arg
		elif opt in ("-p", "--projects"):
		  projects = arg.split(',')
		elif opt in ("-t", "--type"):
		  type = arg
		elif opt in ("-i", "--input"):
		  staticRootDir = arg

    if os.path.exists('download') :
            shutil.rmtree('download')
    os.mkdir('download')

    if os.path.exists(experiment) :
            shutil.rmtree(experiment)
    os.mkdir(experiment)

    downloadId = submitJob(projects, type)
    wait(downloadId)
    download(downloadId, type, experiment)
    copyToHDFS(experiment)
    validate(type,projects,experiment,staticRootDir)

if __name__ == "__main__":
	main(sys.argv[1:])