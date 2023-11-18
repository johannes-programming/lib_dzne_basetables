import string as _str
import sys as _sys

import na_quantors as _na
import pandas as _pd


def string(value, /):
    if _na.isna(value):
        return ""
    if value is False:
        return "N"
    if value is True:
        return "Y"
    value = str(value)
    ans = ""
    for x in value:
        if x in ("\"", "'"):
            ans += f"\\x{hex(ord(x))}"
        if 32 <= ord(x) < 127:
            ans += x
        else:
            ans += ascii(x)[1:-1]
    return ans

def check_column(col):
    assert type(col) is str, f"Column name must be a string! {col.__repr__()} is invalid. "
    assert col != "", "Empty column name is invalid! "
    assert col[0] not in _str.digits, "Column name can not start with a numeral! "
    for ch in col:
        msg = f"Column name {col.__repr__()} contains invalid character {ch.__repr__()} (ASCII-Code = {ord(ch)})! "
        assert ch in f"{_str.ascii_uppercase}_{_str.digits}", msg
    return col 



def identify_columns(table, *, patterns):
    assert type(table) is _pd.DataFrame
    return _pat.select(list(table.columns), patterns)

def any_columns(table, *, patterns):
    return bool(len(identify_columns(table, columns=columns)))

def fuse(*dicts, reduce=False):
    assert reduce in (False, True)
    valuess = dict()
    for dB in dicts:
        for k, v in dB.items():
            if k not in valuess.keys():
                valuess[k] = list()
            if v != "":
                valuess[k].append(v)
    ans = dict()
    for k, values in valuess.items():
        s = set(values)
        assert len(s) <= 1, "Item {item} is defined as {text}! ".format(
            item=ascii(str(k)),
            text=" and as ".join(ascii(str(x)) for x in list(s)),
        )
        if 0 < len(s):
            ans[k] = values[0]
        elif (not reduce):
            ans[k] = ""
    return ans






