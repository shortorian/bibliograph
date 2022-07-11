{
    "author": {
        "note": "Source has author target. Target is author of source."
    },

    "superauthor": {
        "note": "Source has superauthor target. Target is superauthor "
                "of source. A superauthor is an actor associated with a "
                "work whose content is potentially written by other "
                "actors, for instance a book editor or a dissertation "
                "committee member."
    },

    "title": {
        "note": "Source has title target. Target is title of source."
    },

    "supertitle": {
        "note": "Source has supertitle target. Target is supertitle of "
                "source. A supertitle is a name for a work that "
                "contains another work. For instance a book title is "
                "the supertitle of a chapter and a journal title may be "
                "the supertitle of an article. "
    },

    "contained": {
        "note": "Source contains target. Target is contained by source."
    },

    "volume": {
        "note": "Source has volume target. Target is volume of source. "
                "A link of type 'volume' should be associated with a "
                "work that represents one volume of some larger work. "
                "This does not map unambiguously to a 'contains' link "
                "because a journal could be the source of a link with "
                "type 'volume' but an article could also be the source "
                "of a link with type 'volume'. Links of type 'contains' "
                "should be assigned separately and unambigously when a "
                "'volume' link is created."
    },

    "number": {
        "note": "Source has number target. Target is number of source. "
                "A link of type 'number' should be associated with a "
                "work that represents one volume of some larger work, "
                "like a chapter in a book or an issue of a periodical. "
                "This does not map unambiguously to a 'contains' link "
                "because a journal could be the source of a link with "
                "type 'number' but an article could also be the source "
                "of a link with type 'number'. Links of type "
                "'contains' should be assigned separately and "
                "unambigously when a 'number' link is created."
    },

    "page": {
        "note": "Source has page target. Target is page of source. A "
                "link of type 'page' should be associated with a "
                "work that represents one page of some larger work. "
                "Whether or not this maps directly to a 'contains' link "
                "depends on the user's convention with regard to texts. "
                "If an article or chapter is a concrete object that "
                "contains pages, then a citation with a page reference "
                "can be unambiguously resolved into 'contains' links. "
                "If an article or chapter is an abstract text object, "
                "then it may not make sense to assign it 'contains' "
                "links for pages."
    },

    "endpage": {
        "note": "Source has page target. Target is page of source. A "
                "link of type 'endpage' should be associated with a "
                "work that represents one page of some larger work. "
                "Whether or not this maps directly to a 'contains' link "
                "depends on the user's convention with regard to texts. "
                "If an article or chapter is a concrete object that "
                "contains pages, then a citation with a page reference "
                "can be unambiguously resolved into 'contains' links. "
                "If an article or chapter is an abstract text object, "
                "then it may not make sense to assign it 'contains' "
                "links for pages."
    },

    "affiliation": {
        "note": "Source has affiliation target. Target is affiliation "
                "of source."
    },

    "citation": {
        "note": "Source cites target. Target is cited by source"
    },

    "acknowledgement": {
        "note": "Source acknowledges target. Target is acknowledged by "
                "source."
    },

    "date_published": {
        "note": "Source has publication date target. Target is date on "
                "which source was published.",
    },

    "funder": {
        "note": "Source has funder target. Target is funder of source. "
                "This is intended to represent generic funding "
                "relationships regardless of node type. This link type "
                "could connect actors with agreements, actors with "
                "actors, actors with works, etc. The source of the link "
                "is interpreted as the source of funds, so an actor "
                "paying for an agreement will be a source of a link to "
                "an agreement while an actor funded through an "
                "agreement will be the target of a link from an "
                "agreement."
    },

    "alias": {
        "note": "Source is alias of target. Target is alias of source."
    },

    "annotated": {
        "note": "Source has annotation target. Target is annotation of source."
    }
}
