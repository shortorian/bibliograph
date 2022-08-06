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
    partial_surnames=['st', 'de', 'le', 'van', 'von'],
    exclusions=None
):

    if exclusions is None:
        exclusions = [
            'administration',
            'university',
            'national',
            'institute',
            'center'
        ]

    drop_nouns = pd.Series(drop_nouns)
    drop_nouns = pd.concat([drop_nouns, drop_nouns.map(lambda x: x + '.')])

    generationals = pd.Series(generationals)
    generationals = pd.concat([
        generationals,
        generationals.map(lambda x: x + '.')
    ])

    partial_surnames = partial_surnames + [p + '.' for p in partial_surnames]

    concat_words_regex = r'[^\w]|[\d_]'
    first_letters_regex = r'(?!\b)\w*|\W*?'

    split_names = name_series.str.contains(',')
    plural_names = name_series.str.contains(' ')
    plural_names = plural_names & ~split_names

    name_excluded = pd.Series(
        sum([name_series.str.casefold().str.contains(s) for s in exclusions]),
        dtype=bool
    )
    split_names = split_names & ~name_excluded
    plural_names = plural_names & ~name_excluded

    if split_names.any():

        split_names = name_series.copy().loc[split_names]
        split_names = split_names.str.casefold()

        print(split_names)

        split_names = split_names.str.split(',', expand=True)
        split_names = split_names.apply(lambda x: x.str.strip())

        if len(split_names.columns) > 2:
            more_fields = split_names[2].notna()

        else:
            more_fields = pd.Series(False, index=split_names[0].index)

        split_names = split_names[[0, 1]]

        is_drop_noun = split_names[1].isin(drop_nouns)

        if is_drop_noun.any():

            selection = split_names.loc[is_drop_noun & ~more_fields, 0].copy()
            selection = selection.str.rsplit(' ', n=1, expand=True)

            split_names.loc[selection.index, 0] = selection[1]
            split_names.loc[selection.index, 1] = selection[0]

            split_names.loc[is_drop_noun & more_fields, 1] = pd.NA

        is_generational = split_names[1].isin(generationals)

        if is_generational.any():

            gens = split_names[1].loc[is_generational & ~more_fields]

            selection = split_names.loc[is_generational & ~more_fields, 0]
            selection = selection.str.rsplit(' ', n=1, expand=True)
            slctn_idx = selection.index

            split_names.loc[slctn_idx, 0] = selection[1]
            split_names.loc[slctn_idx, 1] = selection[0]
            given_names = split_names.loc[slctn_idx, 1]
            split_names.loc[slctn_idx, 1] = given_names + ' ' + gens

            split_names.loc[is_generational & more_fields, 1] = pd.NA

        for m in drop_nouns:
            split_names = split_names.apply(lambda x: x.str.removeprefix(m))
            split_names = split_names.apply(lambda x: x.str.removesuffix(m))

        split_names = split_names.apply(lambda x: x.str.strip())

        for p in partial_surnames:

            endswith_p = split_names[1].str.endswith(' ' + p).fillna(False)

            second_parts = split_names.loc[endswith_p, 0]
            split_names.loc[endswith_p, 0] = p + ' ' + second_parts
            given_names = split_names.loc[endswith_p, 1]
            split_names.loc[endswith_p, 1] = given_names.str.slice(
                stop=-len(p)
            )

        split_names[0] = split_names[0].str.replace(
            concat_words_regex,
            '',
            regex=True
        )
        split_names[1] = split_names[1].str.replace(
            first_letters_regex,
            '',
            regex=True
        )

    else:

        split_names = pd.DataFrame(columns=[0, 1])

    if plural_names.any():

        plural_names = name_series.copy().loc[plural_names]

        plural_names = plural_names.str.rsplit(' ', n=1, expand=True)
        plural_names = plural_names.apply(lambda x: x.str.strip())

        endswith_drop_noun = plural_names[1].isin(drop_nouns)
        if endswith_drop_noun.any():
            adjust_names = plural_names.loc[endswith_drop_noun, :]
            adjust_names = adjust_names.str.rsplit(' ', n=1, expand=True)
            adjust_names = adjust_names.apply(lambda x: x.str.strip())
            plural_names.loc[endswith_drop_noun, :] = adjust_names.to_numpy()

        endswith_generational = plural_names[1].isin(generationals)
        if endswith_generational.any():
            adjust_names = plural_names.loc[endswith_generational, :]
            adjust_names = adjust_names.str.rsplit(' ', n=1, expand=True)
            adjust_names = adjust_names.apply(lambda x: x.str.strip())
            generationals = plural_names[endswith_generational, 1]
            adjust_names[0] = adjust_names[0] + ' ' + generationals
            plural_names.loc[endswith_generational, :] = adjust_names.to_numpy()

        adjust_names = plural_names[0].str.rsplit(' ', n=1, expand=True)
        if len(adjust_names.columns) > 1:
            has_partial = adjust_names[1].isin(partial_surnames)
            if has_partial.any():
                second_parts = plural_names[has_partial, 1]
                adjust_names = adjust_names.apply(lambda x: x.str.strip())
                adjust_names[1] = adjust_names[1] + ' ' + second_parts
                plural_names.loc[has_partial, :] = adjust_names.to_numpy()

        surnames = plural_names[1].str.replace(
            concat_words_regex,
            '',
            regex=True
        )
        plural_names[1] = plural_names[0].str.replace(
            first_letters_regex,
            '',
            regex=True
        )
        plural_names[0] = surnames

    else:

        plural_names = pd.DataFrame(columns=[0, 1])

    aliases = pd.concat([split_names, plural_names])
    aliases = (aliases[0] + aliases[1]).str.casefold()
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
