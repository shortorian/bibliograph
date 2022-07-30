import pandas as pd


def western_surname_alias_generator_serial(
    name,
    drop_nouns=['ms', 'mr', 'dr'],
    generationals=['jr', 'sr'],
    partial_surnames=['st', 'de', 'le', 'van', 'von']
):

    if ',' not in name:
        return pd.NA

    name = name.casefold()

    drop_nouns = [s for s in drop_nouns if s in name]
    drop_nouns = [s + '.' if s + '.' in name else s for s in drop_nouns]

    generationals = [s for s in generationals if s in name]
    generationals = [s + '.' if s + '.' in name else s for s in generationals]

    partial_surnames = [s for s in partial_surnames if s in name]
    partial_surnames = [
        s + '.' if s + '.' in name else s for s in partial_surnames
    ]

    name = name.split(',')
    name = [n.strip() for n in name]

    if name[1] in drop_nouns:

        if len(name) == 2:
            name = name[0].rsplit(' ', maxsplit=1)
            name = [name[1], name[0]]

        else:
            return pd.NA

    if name[1] in generationals:

        if len(name) == 2:
            g = name[1]
            name = name[0].rsplit(' ', maxsplit=1)
            name = [name[1], name[0]]
            name[1] = name[1] + ' ' + g

        else:
            return pd.NA

    for m in drop_nouns:
        name = [n.removeprefix(m) for n in name]
        name = [n.removesuffix(m) for n in name]

    name = [n.strip() for n in name]

    for p in partial_surnames:

        if name[1].endswith(' ' + p):

            name[0] = p + ' ' + name[0]
            name[1] = name[1][:-len(p)]

    name[0] = ''.join([c for c in name[0] if c.isalpha()])

    name[1] = [
        s.strip()[0]
        for substring in name[1].split(' ')
        for s in substring.split('-')
        if s != ''
    ]

    return (name[0] + ''.join(name[1]))


def western_surname_alias_generator_vector(
    name_series,
    drop_nouns=['ms', 'mrs', 'mr', 'dr', 'sir', 'dame'],
    generationals=['jr', 'sr'],
    partial_surnames=['st', 'de', 'le', 'van', 'von']
):

    names = name_series.copy().loc[name_series.str.contains(',')]

    names = names.str.casefold()

    names = names.str.split(',', expand=True)
    names = names.apply(lambda x: x.str.strip())

    if len(names.columns) > 2:
        more_fields = names[2].notna()

    else:
        more_fields = pd.Series(False, index=names[0].index)

    names = names[[0, 1]]

    drop_nouns = pd.Series(drop_nouns)
    drop_nouns = pd.concat([drop_nouns, drop_nouns.map(lambda x: x + '.')])
    is_drop_noun = names[1].isin(drop_nouns)

    if is_drop_noun.any():

        selection = names[0].loc[is_drop_noun & ~more_fields].copy()
        selection = selection.str.rsplit(' ', n=1, expand=True)

        names[0].loc[selection.index] = selection[1]
        names[1].loc[selection.index] = selection[0]

        names[1].loc[is_drop_noun & more_fields] = pd.NA

    generationals = pd.Series(generationals)
    generationals = pd.concat([
        generationals,
        generationals.map(lambda x: x + '.')
    ])
    is_generational = names[1].isin(generationals)

    if is_generational.any():

        gens = names[1].loc[is_generational & ~more_fields].copy()

        selection = names[0].loc[is_generational & ~more_fields].copy()
        selection = selection.str.rsplit(' ', n=1, expand=True)
        slctn_idx = selection.index

        names[0].loc[slctn_idx] = selection[1]
        names[1].loc[slctn_idx] = selection[0]
        names[1].loc[slctn_idx] = names[1].loc[slctn_idx] + ' ' + gens

        names[1].loc[is_generational & more_fields] = pd.NA

    for m in drop_nouns:
        names = names.apply(lambda x: x.str.removeprefix(m))
        names = names.apply(lambda x: x.str.removesuffix(m))

    names = names.apply(lambda x: x.str.strip())

    partial_surnames = partial_surnames + [p + '.' for p in partial_surnames]

    for p in partial_surnames:

        endswith_p = names[1].str.endswith(' ' + p).fillna(False)

        names[0].loc[endswith_p] = p + ' ' + names[0].loc[endswith_p]
        names[1].loc[endswith_p] = names[1].loc[endswith_p].str.slice(
            stop=-len(p)
        )

    names[0] = names[0].str.replace(r'[^\w]|[\d_]', '', regex=True)
    names[1] = names[1].str.replace(r'(?!\b)\w*|\W*?', '', regex=True)

    aliases = (names[0] + names[1]).str.casefold()
    aliases = pd.concat([
        aliases,
        pd.Series(pd.NA, index=name_series.index.difference(aliases.index))
    ])

    return aliases.sort_index()


def doi_alias_generator(doi_series, delimiters=['doi:', 'doi.org/', 'doi/']):

    has_delimiter = pd.concat(
        [doi_series.str.contains(d).rename(d) for d in delimiters],
        axis='columns'
    )

    has_delimiter = has_delimiter.apply(
        lambda x: pd.Series(x.name, index=has_delimiter.index).where(x)
    )
    has_delimiter = has_delimiter.ffill(axis='columns')
    has_delimiter = has_delimiter[has_delimiter.columns[-1]]

    output = pd.concat(
        [doi_series.rename('string'), has_delimiter.rename('delimiter')],
        axis='columns'
    )

    def delimiter_splitter(delimiter_group):

        delimiter = delimiter_group.name

        if pd.isna(delimiter):
            return pd.DataFrame(
                {
                    'string': pd.NA,
                    'delimiter': delimiter
                },
                index=delimiter_group.index
            )

        else:
            strings = delimiter_group['string'].str.split(
               delimiter,
               expand=True
            )
            return pd.DataFrame({
                'string': strings[1].str.strip(),
                'delimiter': delimiter
            })

    output = output.groupby(by='delimiter', dropna=False)
    output = output.apply(delimiter_splitter)

    return output['string'].rename(None)


'''
names = pd.Series([
    'Loon, H. van',
    'van Loon, h.',
    'van Loon, Harry',
    'VAN LOON, H',
    'Van loon, ',
    'some other person',
    'Rodríguez-Silva, Ileana',
    'nasa',
    'Martin Luther King, jr.',
    'King, Martin Luther jr.',
    'Mr. Martin Luther King, jr.',
    'St. Whatever, Given Name',
    'Whatever, Given Name St.',
    'University of Washington, Seattle',
    'University of Chicago',
    'Ms. Gerould, Joanne',
    'Gerould, Ms. Joanne',
    'Gerould, Joanne, Ms.',
    'Joanne Gerould, Ms.',
    'Surname, Compound Given-Name',
    'Monde, Alice le',
    'le Monde, Alice'
])

serial = names.map(western_surname_alias_generator_serial)
vector = western_surname_alias_generator_vector(names)
((serial == vector) | (serial.isna() & vector.isna())).all()

identifiers = pd.Series([
    'xxx',
    'yyy',
    'zzz',
    'doi:yyy',
    'https://doi.org/zzz',
    'doi/yyy'
])

(doi_alias_generator(identifiers) == ['yyy', 'zzz', 'yyy']).all()
'''
