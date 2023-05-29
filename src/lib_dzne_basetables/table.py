# importing
import string
import sys
from collections import defaultdict

import pandas as pd

import lib_dzne_basetables.common as _comm
import lib_dzne_basetables.pattern as _pat


def identify_columns(table, *, patterns):
    assert type(table) is pd.DataFrame
    return _pat.select(list(table.columns), patterns)




def augment(table, *, aux, columns, demand):
    check(table)
    check(aux)

    auxrowss = defaultdict(list)
    for i, row in aux.iterrows():
        ID = tuple(row.get(col, "") for col in columns)
        auxrowss[ID].append(row)

    rows = list()
    for i, row in table.iterrows():
        ID = tuple(row[col] for col in columns)
        if demand and (0 == len(auxrowss[ID])):
            continue
        try:
            rows.append(_comm.fuse(row, *auxrowss[ID]))
        except AssertionError as err:
            info = dict(zip(list(columns), list(ID)))
            raise KeyError(f"{info} cannot be unified: {err}")
    return make(rows)


    

def make(data=None, *, basetype=None):
    if data is None:
        data = {}
    if type(data) is pd.DataFrame:
        table = data.copy()
    else:
        table = pd.DataFrame(data)
    table = table.applymap(_tostr)
    check(table, basetype=basetype)
    return table




def concat(tables):
    ans = pd.concat(tables, axis=0, ignore_index=True)
    ans = make(ans)
    return ans




def check_column(col):
    assert type(col) is str, f"Column name must be a string! {ascii(col)} is invalid. "
    assert col != "", "Empty column name is invalid! "
    assert col[0] not in string.digits, "Column name can not start with a numeral! "
    for ch in col:
        msg = f"Column name {ascii(col)} contains invalid character {ascii(ch)} (ASCII-Code = {ord(ch)})! "
        assert ch in f"{string.ascii_uppercase}_{string.digits}", msg
    return col




def check(table, *, basetype=None):
    assert basetype in (None, 'a', 'd', 'y', 'c', 'm')
    cols = list(table.columns)
    dc = list()
    for col in cols:
        assert col not in dc, f"Column name {ascii(col)} occures more than once! "
        dc.append(col)
        check_column(col)
    if table.size == 0:
        return
    table.applymap(_check_str)
    if 'GROUP' in table.columns:
        assert 'all' not in table['GROUP'].tolist()
    pcr2_missing = "pcr2-information missing! "
    plasmid_missing = "plasmid-information missing! "
    ct_illegal = ("Illegal value in column 'CHAIN_TYPE'! ")
    if basetype in ('d', 'y', 'c'):
        assert ('PCR2' in cols), pcr2_missing
        if 'PLASMID' in table.columns:
            for plasmid in table['PLASMID'].tolist():
                try:
                    assert type(float(plasmid)) is not float, "Improper plasmid name! "
                except ValueError:
                    continue
    if basetype in ('a', 'd', 'm'):
        for IDcol in ('COLLECTION', 'NR'):
            assert IDcol in cols, f"IDcolumn '{IDcol}' is required! "
    if basetype == 'a':
        assert 'CHAIN_TYPE' not in cols, ("Column 'CHAIN_TYPE' not allowed! ")
        assert 0 != len(identify_columns(
            table, 
            patterns=["H_*", "K_*", "L_*"],
        )), "No chain-type-specific information found! "
        assert 0 != len(identify_columns(
            table,
            patterns=['H_PCR2', 'H_PCR2_1', 'K_PCR2', 'K_PCR2_1', 'L_PCR2', 'L_PCR2_1'],
        )), "No chain-type-specific pcr2 found! "
        assert 0 == len(identify_columns(
            table, 
            patterns=['PCR2', 'PCR2_*'],
        )), "Not chain-type-specific pcr2-information found! "
        assert 0 == len(identify_columns(
            table, 
            patterns=['PLASMID', 'PLASMID_*'],
        )), "No plasmid-information allowed! "
    if basetype == 'd':
        assert 'CHAIN_TYPE' in cols, "Column 'CHAIN_TYPE' is required! "
        assert 'PCR2' in cols, "Column 'PCR2' is required! "
        assert all(table['CHAIN_TYPE'].isin(['HC', 'KC', 'LC', 'N/A'])), ct_illegal
        _unique_constraint(table, columns=['PCR2'])
        if 'CN' in cols:
            assert all(
                (table['CHAIN_TYPE'] != 'HC') | (table['CN'] == "")
            ), "Heavy chains are not allowed to have CN-values! "
    if basetype == 'y':
        assert 0 != len(identify_columns(
            table, 
            patterns=['PLASMID', 'PLASMID_1'],
        )), plasmid_missing
        n = 1
        while f"PLASMID_{n}" in table.columns:
            for plasmid in table[f"PLASMID_{n}"].tolist():
                try:
                    assert type(float(plasmid)) is not float, "Improper plasmid name! "
                except ValueError:
                    continue
            n += 1
        if 'CHAIN_TYPE' in cols:
            assert all(table['CHAIN_TYPE'].isin(["", 'HC', 'KC', 'LC', 'N/A'])), ct_illegal
    if basetype == 'c':
        assert 'PLASMID' in cols, plasmid_missing
        _unique_constraint(table, columns=['PLASMID'])
        for i, row in table.iterrows():
            assert row.get('CHAIN_TYPE', "") in ["", 'HC', 'KC', 'LC', 'N/A'], f"error: {row.to_dict()} "
    if basetype == 'm':
        for IDcol in ('LIGHT_CHAIN_TYPE', ):#'CN'):
            assert IDcol in cols, f"The column '{IDcol}' is required! "
        assert 'CHAIN_TYPE' not in cols, ("Illegal column 'CHAIN_TYPE'! ")
        for CT in table['LIGHT_CHAIN_TYPE']:
            assert (
                CT in ["", 'KC', 'LC']
            ), f"Illegal value {ascii(CT)} in column 'LIGHT_CHAIN_TYPE'! "
        _unique_constraint(
            table, columns=('COLLECTION', 'NR', 'LIGHT_CHAIN_TYPE')
        )




def compress(table, *, basetype):
    check(table=table)
    dd = {
        None: None,
        'a': None,
        'd': ['PCR2'],
        'y': None,
        'c': ['PLASMID'],
        'm': ['COLLECTION', 'NR', 'LIGHT_CHAIN_TYPE'],
    }
    table = unify(
        table=table,
        columns=dd[basetype],
        purge=True
    )
    check(table=table, basetype=basetype)
    return table




def unify(table, *, columns, purge=False):
    if columns is None:
        return table
    columns = identify_columns(table, patterns=columns)
    rdss = defaultdict(list)
    for i, row in table.iterrows():
        rd = row#.to_dict()
        ID = tuple(rd.get(col, "") for col in columns)
        rdss[ID].append(rd)
    if purge:
        ID = ("",) * len(columns)
        rdss.pop(ID, None)
    rows = list()
    for ID, rds in rdss.items():
        try:
            rows.append(_comm.fuse(*rds, reduce=True))
        except AssertionError as err:
            info = dict(zip(list(columns), list(ID)))
            print(
                f"{info} cannot be unified: {err}",
                file=sys.stderr,
            )
    return make(rows)




def _tostr(x):
    if x is True:
        return 'Y'
    if x is False:
        return 'N'
    if x is None:
        return ""
    if pd.isna(x):
        return ""
    allowedchars = set(string.letters + string.punctuation + " ") - set("'\"")
    forbiddenchars = set(x) - set(allowedchars)
    if len(forbiddenchars):
        raise ValueError(f"The chars {forbiddenchars} are not allowed in a table! ")
    return str(x).strip()


def _unique_constraint(df, columns=[]):
    keys = list()
    duplicates = set()
    for i, row in df.iterrows():
        key = tuple(row[col] for col in columns)
        if key in keys:
            if key not in duplicates:
                duplicates |= {key}
        else:
            keys.append(key)
    if len(duplicates):
        errors = [KeyError(f"The key {dict(zip(columns, key))} is not unique! ") for key in duplicates]
        raise GroupException(errors)

def _check_str(x):
    assert x == _tostr(x), f"{ascii(str(x))} {type(x)}"
    return x


