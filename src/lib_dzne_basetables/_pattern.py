"""\
This module concers itself with patternrecognition. \
The asterisk is always allowed to represent any amount of chars \
with a finite length greater or equal to zero. \
"""

def select(pool, patterns):
    """Select items in the pool that fit at least one of the patterns. """
    if len(pool) != len(set(pool)):
        for index, item in enumerate(pool):
            if item in pool[index+1:]:
                raise ValueError(f"'{item}' occures more than once! ")
    ans = list()
    for text in pool:
        for pattern in patterns:
            if isfit(text=text, pattern=pattern):
                if text not in ans:
                    ans.append(text)
    return ans

def isfit(*, text, pattern):
    """Tries to fit text to pattern. Returns boolean value. """
    try:
        fit(text, pattern)
    except ValueError:
        return False
    return True
def fit(text, pattern):
    """Tries to fit text to pattern. Returns list of values that have replaced the asterisks. """
    def gen():
        s = text
        l = pattern.split('*')
        if len(l) == 1:
            if l[0] != s:
                raise ValueError()
            return
        if not s.startswith(l[0]):
            raise ValueError()
        s = s[len(l[0]):]
        for x in l[1:-1]:
            i = s.index(x)
            yield s[:i]
            s = s[i+len(x):]
        if not s.endswith(l[-1]):
            raise ValueError()
        if l[-1] != "":
            s = s[:-len(l[-1])]
        yield s
    return list(gen())





