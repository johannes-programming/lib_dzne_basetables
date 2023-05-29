import lib_dzne_basetable as _bt
import lib_dzne_tsv as _tsv


class BASEData(_tsv.TSVData):
    @classmethod
    def basetype(cls):
        if cls._ext.startswith("."):
            raise ValueError()
        if cls._ext.startswith("base"):
            raise ValueError()
        return cls._ext[1:-4]
    @classmethod
    def _load(cls, /, file, **kwargs):
        ans = super().load(file, **kwargs).dataFrame
        ans = _bt.table.make(ans, basetype=cls.basetype())
        return ans
    def _save(self, /, file):
        data = _bt.table.make(data, basetype=self.basetype())
        super().save(string, data)
    @classmethod
    def _default(cls):
        return _bt.table.make(basetype=cls.basetype())
    @classmethod
    def from_file(cls, file, /):
        return super().from_file(file, ABASEData, CBASEData, DBASEData, MBASEData, YBASEData)
    @classmethod
    def clone_data(cls, data, /):
        data = super().clone_data(data)
        return _bt.table.make(data, basetype=cls.basetype())
class ABASEData(BASEData):
    _ext = ".abase"
class CBASEData(BASEData):
    _ext = ".cbase"
class DBASEData(BASEData):
    _ext = ".dbase"
class MBASEData(BASEData):
    _ext = ".mbase"
class YBASEData(BASEData):
    _ext = ".ybase"