import sys



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




