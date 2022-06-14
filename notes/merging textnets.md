nodes in different textnets could be duplicates if

1. they have the same metadata
2. one or both nodes have no non-nan metadata values

resolving case 2 requires comparing string representations of nodes

steps:
- reindex subset of tables in the first textnet so they overlap and/or continue indexes in the other textnet
	1. make index maps for 
		- strings
		- link_types
		- node_types
		- tags
		- comments
	2. apply index maps to assertions table
	3. if all assertions from one source in the first textnet are duplicated for a single source in the second textnet, drop all assertions from source in the second textnet and propagate deletion.
		### Need to propagate deletion of a source
		
	