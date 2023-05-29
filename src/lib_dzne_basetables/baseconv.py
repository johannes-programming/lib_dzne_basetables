
import sys
from collections import defaultdict

from lib_dzne_basetables import common as _comm
from lib_dzne_basetables import pattern as _pat
from lib_dzne_basetables import table as _tbl


def a2d(table):
    """convert aTable to dTable"""
    table = _deconstruct_by_number(
        table, 
        purge=False,
    )
    CG = dict()
    for ct in "HKL":
        CG[f"{ct}C"] = f"{ct}_*"
    ans = _deconstruct(
        table, 
        patterns=CG, 
        outcolumn='CHAIN_TYPE',
        purge=False,
        basetype='d',
    )
    return ans


def y2c(table):
    """convert yTable to cTable"""
    ans = _deconstruct_by_number(
        table, 
        purge=False,
        basetype='c',
    )
    return ans


def _deconstruct(table, *, patterns, outcolumn, purge, basetype):
    maincols = list(table.columns)
    NF = dict()
    for outsign, pattern in patterns.items():
        mvcols = dict()
        _maincols = list()
        for oldcol in maincols:
            try:
                l = _pat.fit(oldcol, pattern)
            except ValueError:
                _maincols.append(oldcol)
            else:
                newcol = "".join(l)
                mvcols[oldcol] = newcol
        maincols = _maincols
        NF[outsign] = mvcols
    if len(maincols) == len(list(table.columns)):
        return table
    rz = list()
    for i, row in table.iterrows():
        for outsign, mvcols in NF.items():
            rdA = {k: row[k] for k in maincols}
            rdB = {v: row[k] for k, v in mvcols.items()}
            if all(v == "" for v in rdB.values()) and purge:
                continue
            rdC = dict()
            if outcolumn != "":
                rdC[outcolumn] = outsign
            rz.append(_comm.fuse(rdA, rdB, rdC, reduce=True))
    ans = _tbl.make(data=rz)
    if basetype is not None:
        ans = _tbl.compress(
            table=ans,
            basetype=basetype,
        )
    return ans    


def _deconstruct_by_number(table, *, purge, basetype=None):
    dm = 1
    while 0 < len(_tbl.identify_columns(table, patterns=[f"*_{dm}"])):
        dm += 1
    dm -= 1
    patterns = dict()
    for n in range(1, dm + 1):
        patterns[str(n)] = f"*_{n}"
    ans = _deconstruct(
        table, 
        patterns=patterns, 
        outcolumn="",
        purge=purge,
        basetype=basetype,
    )
    return ans



