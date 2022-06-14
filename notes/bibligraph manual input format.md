# This note describes the format of a bibliograph manual data entry file
note created 2022_02_24
TODO: indicate the source and target nodes for each assertion described below

The overall file format is csv. A line in a bibliograph input csv file consists of three values

1. An entry in the first csv column
2. An entry in the second column
3. A whitespace-delimited set of tags and a link type whose value begins `lt__` (both optional)

Line format:
	`entry(__ tag tag ...), entry(__ tag tag ...), (lt__link_type tag tag ...)`

Each entry consists of one or more string values which will generate nodes, assertions, and notes in the bibliograph database as described below. As of 2022_02_24, entries in the first column are always interpreted as works which contain information recorded in the second column.

The optional link type in the third csv column overrides the default the link type for the relation between entries (as discussed in entry formats below). The optional whitespace-delimited list of tags generates one comment attached to the link between entries for each tag. Comments contain the text "tag:X" where X is the string value of the tag.

Each `entry` is interpreted as a double-underscore-delimited list, so whitespace is allowed within an entry, with one exception: __list items within an entry cannot bein with a space.__ A list separator followed by a space indicates the end of the entry and the beginning of the tags. If a space is required at the beginning of an item within an entry, use a placeholder as described below.

When entering large amounts of data, it is recommended to use placeholders for whitespace within entries so that a text editor can provide automatic tab-completions for entire entries. By default bibliograph will replace pipes, |, with single spaces unless they are escaped with a backslash. (Note that you will probably need to change your editor's settings so words do not break on pipes or any other non-whitespace characters that will be commonly included in entries).

To process text you might want to use repeatedly that has many spaces, transcribe the text with spaces and then search-and-replace within the transcription to convert the spaces to placeholders.

### Lines in a manually transcribed file will frequently be incomplete.

When working through many sources, it is recommended to write one entry for the source followed by a comma, then a set of lines below it that begin with a comma, assuming lines with no first value refer to the most recent line with a value in the first column. A transcript of sources and works referenced in each source might look like this

`entry__for__first__source source_tag_one source_tag_two,`
`    , entry__for__first__citation some_tag_for_works`
`    , entry__for__second__citation some_tag_for_works another_tag_for_works
`    , entry__for__third__citation`
`entry__for__second__source,`
`    , entry__for__first__citation`
`    , entry__for__second__citation a_tag_for_works, a_tag_for_links`

In this case lines 2-4 contain entries in the second csv column whose first csv column value is provided in the top line, while lines 6 and 7 are cited in the source on line 5. The last line contains a work cited as well as a tag attached to the link between the works.

If this abbreviated entry format is used, then bibliograph will raise an error if any line has a value in the first column and a value in the second or third columns. Each line must either be a line for a source entry (with a value in the first csv column and no others) or a line for a target entry and optional information about the link between the entries (with a value in the second and possibly third csv columns but no value in the first column.

### Formats for the first and second CSV columns

__Entries with no prefix and whose first item is missing (represented by `x`) must begin `__x__`. Entries beginning `x__` will be interpreted as having the prefix `x`.__

__Work, allowed in first or second csv column:__
    `(w__ or none)(actor_actor...)__year__(s_ or t_ or none)title__volume__page(__doi) (tag tag...)`
__Except for the optional DOI, all missing values in the underbar delimited string (actor__year__title__volume__page) must be denoted `x`__ 
Database objects linked or created:
  - One work node for the entry. If this entry is in the second csv column, this work node is asserted cited by the work node created for the first entry
  - One actor node per actor, each asserted author of work node created for this entry
  - One date node for the year, asserted date of work node created for this entry
  - One work node for the title __IF__ the title begins `s_` __OR__ if it does not begin with a single character followed by an underscore. This is used to represent journals or other publications containing this work. If the title begins `t_` then no additional node is created and the title is interpreted as the title of the work represented by this entry rather than the title of a work containing the work represented by this entry.
  - One work node for the volume, asserted volume of the work node created for this entry
  - One work node for the page, asserted page of the work node created for this entry
  - One identifier for the DOI, asserted DOI of the work node created for this entry (optional)
  - Note with text "tag:X" for each tag, each attached to the work node created for this entry (where X is the tag value in the entry)
  - If this is entry is in the second csv column, an assertion with link type citation is created between the work nodes created for the entries in the first and second csv columns __unless__ there is a value beginning `lt__` in the third csv column, which will override the link type for the assertion between work nodes created for the entries in the first and second csv columns

__Funder, allowed in second csv column only:__
	`f__(actor_actor...)__agreement tag tag ...`
Database objects linked or created:
  - One agreement node, asserted funder of the work node created for the entry in the first csv column
  - One actor node per actor, each asserted funder of agreement node and funder of the work node created for the entry in the first csv column
  - Note with text "tag:X" for each tag, each attached to the agreement node (where X is the tag value in the entry)                                                                                                                                             
  - If a value beginning `lt__` is present in the third csv column, all assertions above will have the link type listed there instead of link type "funder". 

__Acknowledgement, allowed in second csv column only:__
	`(k__ or none)(actor_actor...) tag tag ...`
Database objects linked or created:
  - One actor node per actor, asserted acknowledged in the work node created for the entry in the first csv column
  - Note with text "tag:X" for each tag, each attached to every actor node (where X is the tag value in the entry)
  - If a value beginning `lt__` is present in the third csv column, all assertions above will have the link type listed there instead of link type "acknowledged". 

__Organization, allowed in second csv column only:__
	`o__(actor_actor...) tag tag`
	Where the number of actors is the same as the number of authors for the work in the first entry. Actor nodes created for authors in the first entry are asserted affiliated with each actor node created for this entry, matched one-to-one in the order listed in the first entry. Missing organizations must be denoted `x`.
Database objects linked or created:
  - One actor node per actor, asserted affiliated with the corresponding actor node identified in the first entry __AND__ asserted affiliated with the work node created for the first entry
  - Note with text "tag:X" for each tag, each attached to every actor node (where X is the tag value in the entry)
  - If a value beginning `lt__` is present in the third csv column, all links above will have the link type listed there instead of link type "acknowledged". 

__Quote, allowed in second csv column only:__
	`q__quote text__tag tag
Database objects linked or created:
  - One quote node asserted contained in the work node created for the first entry
  - Note with text "tag:X" for each tag, each attached to quote node (where X is the tag text in the entry) 
  - There are no constraints on the text of the quote. __However:__ If the quote text should contain double underscores and the line has no tags associated with it, then you must end the line with a double underscore so the parser interprets the last double underscore in the quote as quoted text instead of a tag separator.
  - If a value beginning `lt__` is present in the third csv column, the link between the work node created for the entry in the first csv column and the quote node will have the link type listed there instead of link type "contained". 
  
__Note, allowed in second csv column only:__
	`n__note text
__No third csv column values are accepted for notes. All text beyond the double underscore will be interpreted as a single note.__
Database objects linked or created:
  - One note containing the text following the double underscore, attached to work listed in entry in the first csv column
  - There are no constraints on the text of the note
