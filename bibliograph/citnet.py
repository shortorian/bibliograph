import json
import networkx as nx
import pandas as pd
from .readwrite import slurpBibTex
from .readwrite import slurpReferenceCSV
from .util import backup
from .util import bibUpdate 
from .util import makeGraph
from .nasaads import queryADSbibcodes
from .nasaads import queryADS
from os.path import isfile

class citnet:
	'''
	The citnet class provides a citation network object that contains
	a pandas DataFrame for a bibliography, a DataFrame for references
	between bibliography, and a NetworkX graph representing the
	network. 

	Parameters
	----------
	name : string
		prefix 

	bibtex : string
		Name of BibTex file containing the bibliography

	bibcols : list-like
		List of labels for bibliography columns

	refcols : list-like OR string
		Labels of columns whose values should be joined by spaces to
		create a unique reference string for each row. If string, must
		contain a column label. Defaults to 'title'.
	
	bibTex_processors : dictionary
		tag_processors is a dictionary with format 
		
			{bibTexTag:[columnName, function_to_process_bibTex]}
		or
			{bibTexTag:[[columnName1, function1_to_process_bibTex],
						[columnName2, function2_to_process_bibTex]]}

		where bibTexTag is a field in the BibTex entries, columnName
		is the label for a bibliography column where data from that
		tag should be stored, and the functions translate the text of
		the BibTex entry into a value that should be stored in the 
		bibliography column

	'''
	# TODO : make abbr an attribute of the citation network?
	def __init__(self, data=None, index=None, bibcols=None, bibtex=None, csv=None, fileprefix=None, refcols='title', bibTex_processors=None, direction='outgoing', uid='ref', noNewSources=False, separator=' | ', translator=None):

		self.bib = pd.DataFrame(data=data, index=index, columns=bibcols, dtype=str)
		self.cit = pd.DataFrame(columns=['src', 'tgt'], dtype='int')

		self.refcols = refcols
		if type(refcols) == str:
			self.uid = refcols
		else:
			self.uid = 'ref'

		if bibtex is not None:
			if (csv is not None) or (fileprefix is not None):
				raise ValueError('citnet is initialized with exactly one of bibtex, csv, or fileprefix. Got at values for at least two.')
			slurpBibTex(self, bibtex, bibcols=bibcols, refcols=refcols, tag_processors=bibTex_processors)

			self.notUnique = [c for c in self.bib if c != self.uid]

			self.bib = self.bib.fillna('x')
			self.graph = makeGraph(self.bib, self.cit, self.uid)

		if csv is not None:
			if (bibtex is not None) or (fileprefix is not None):
				raise ValueError('citnet is initialized with exactly one of bibtex, csv, or fileprefix. Got at values for at least two.')
			print('Loading data from ' + csv)
			slurpReferenceCSV(self, csv, direction, noNewSources, separator, translator)

		if fileprefix is not None:
			if (bibtex is not None) or (csv is not None):
				raise ValueError('citnet is initialized with exactly one of bibtex, csv, or fileprefix. Got at values for at least two.')
			checkBib = isfile(fileprefix + '-bib.json')
			checkCit = isfile(fileprefix + '-cit.json')
			checkGraph = isfile(fileprefix + '.graphml')

			if all([checkBib, checkCit, checkGraph]):
				self.bib = pd.read_json(fileprefix + '-bib.json')
				self.cit = pd.read_json(fileprefix + '-cit.json')
				self.graph = nx.read_graphml(fileprefix + '.graphml')
				self.notUnique = [c for c in self.bib if c != self.uid]
				print('\nNetwork loaded from disk.\n')
			else:
				raise RuntimeError('\nFound at least one stored file for bib, cit, or graph, but did not find all three.')

	def update(self, newEntry, updateCit=False, src=None):
		'''
		Take data for a bibliography entry and either overwrite an
		existing entry with new data, rewrite the same entry, or add
		a new entry to the bibliography.

		Parameters
		----------
		newEntry : pd.Series or pd.DataFrame
			either a pandas Series object or a DataFrame containing a
			single row that can be squeezed into a Series. Contains
			data for a bibliography entry that may or may not already
			exist in the bibliography DataFrame.

		updateCit : boolean
			If True, create new citation edge for this target. 

		src : integer
			Bibliography index of source that references this target.
		'''
		# TODO : make this function update the graph?

		getUpdate = bibUpdate(self.bib, newEntry, self.uid)

		if getUpdate.updated:
			self.bib.loc[getUpdate.index] = getUpdate.entry
			if updateCit and not ((self.cit.src == src) & (self.cit.tgt == getUpdate.index)).any():
				self.cit = self.cit.append({'src':src, 'tgt':getUpdate.index}, ignore_index=True)
		else:
			self.bib = self.bib.append(newEntry, ignore_index=True).fillna('x')
			if updateCit:
				self.cit = self.cit.append({'src':src, 'tgt':self.bib.index[-1]}, ignore_index=True)
	
	def loadCSV(self, filename, **kwargs):
		'''
		Get bibliography and citation data from a csv file.

		Parameters
		----------
		filename : string
			Name of csv file.

		kwargs
			Keyword arguments passed to
			bibliography.readwrite.slurpReferenceCSV
		'''
		print('Loading data from ' + filename)
		slurpReferenceCSV(self, filename, **kwargs)

	def writeNetwork(self, name):
		'''
		Write JSON files representing the bibliography and citation
		DataFrames. Write a graphml file representing the graph.

		Parameters
		----------
		name : string
			Network name will be the prefix for all stored filenames.
		'''

		backup(name + '-bib.json')
		self.bib.to_json(name + '-bib.json')

		backup(name + '-cit.json')
		self.cit.to_json(name + '-cit.json')

		if self.graph:
			backup(name + '.graphml')
			nx.write_graphml(self.graph, name + '.graphml')

	def getADSbibcodes(self, searchColumns, **kwargs):
		'''
		Get ADS bibcodes for papers in the sources DataFrame. If this
		function tries to construct a query for some entry but can't
		(the index for that entry winds up in badQueries), the
		bibcode field for that entry gets '?'.

		Parameters
		----------
		searchColumns : list-like
			Column labels in the sources DataFrame which contain data
			for the ADS search queries.

		kwargs
			Keyword arguments are passed directly to
			bibliograph.nasaads.queryADSbibcodes

		Returns
		-------
		queries : pd.DataFrame
			pandas DataFrame with query strings, ads articles objects 
			retreived from the ADS, and bibcodes from those articles
			objects. queries index corresponds to sources index.

		badQueries : list
			List of index values from the sources DataFrame for which
			values in columns to be searched either contained spaces
			or were 'x'.	
		'''
		queries, badQueries = queryADSbibcodes(self.bib, searchColumns, **kwargs)

		self.bib.loc[queries.index, 'bibcode'] = queries['bibcode']
		self.bib.loc[badQueries, 'bibcode'] = '?'

		return(queries, badQueries)

	def queryADS(self, searchColumns, fetchTerms, **kwargs):
		'''
		Get ADS data for bibliography entries.

		Parameters
		----------
		searchColumns : list-like
			Column labels in the sources DataFrame which contain data
			for the ADS search queries.

		fetchTerms : list-like
			ADS search terms to get for each query. This is a list of
			data to fields to download from the API rather than search
			terms with which to construct the search query from the
			sources	DataFrame. List of ADS terms is in the drop-down
			menu above the search bar at
			https://ui.adsabs.harvard.edu/

		kwargs
			Keyword arguments are passed directly to
			bibliograph.nasaads.queryADSbibcodes

		Returns
		-------
		queries : pd.DataFrame
			pandas DataFrame with query strings, ads articles objects 
			retreived from the ADS, and bibcodes from those articles
			objects. queries index corresponds to sources index.

		badQueries : list
			List of index values from the sources DataFrame for which
			values in columns to be searched either contained spaces
			or were 'x'.	
		'''
		results, queries, badQueries = queryADS(self.bib, searchColumns, fetchTerms, **kwargs)

		uid = self.uid

		for i in results.index:

			srcID = results.loc[i, 'srcidx']
			
			thisResult = results.loc[i, results.columns[:-1]].squeeze().copy()

			if uid == 'ref':
				thisResult[uid] = ' '.join([thisResult[c] for c in self.refcols if (thisResult[c] != 'x')])

			self.update(thisResult, updateCit=True, src=srcID)

		return(results, queries, badQueries)