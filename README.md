# bibliograph
A database system for humanities research projects.

bibliograph is a Python package that implements a unique data model designed to help researchers analyze complex source material. The database was designed for academic studies in history, where researchers are often required to manage large amounts of ambiguous, inconsistent, or potentially contradictory information from a variety of sources. bibliograph provides methods to store relations between entities as they are represented in source material ("assertions" users transcribe or annotate from sources) alongside abstract relations interpreted from the sources (edges between nodes created by a user). Researchers can therefore retain verbatim contents of sources while generating a normalized relational database that's easily queried. This is a solution to the "problem" of normalization: a historian might like to compare sources that mention Ada Lovelace as "Ada Lovelace" to those that use the name "Augusta Byron", but a normalized relational database typically requires awkward linking between entities with different names. bibliograph is designed to avoid normalized references to abstract entities and allow users to find relationships between sources that refer to the same entities in inconsistent or contradictory ways.

bibliograph is in early development, with an alpha release and more detailed documentation expected in summer 2022.

## The Data Model

![A database diagram for the bibliograph ERD](./2022_06_14_bibliographERD.svg)
