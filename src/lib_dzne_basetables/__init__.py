import collections as _collections
import sys as _sys
import warnings as _wng

import lib_dzne_tsv as _tsv

import lib_dzne_basetables._pattern as _pat
import lib_dzne_basetables._utils as _utl

_pcr2_missing = "pcr2-information missing! "
_plasmid_missing = "plasmid-information missing! "
_ct_illegal = "Illegal value in column 'CHAIN_TYPE'! "



class BASEData(_tsv.TSVData):
    _IDcolumns = None
    def columns(self, *patterns):
        return _utl.identify_columns(self.data, patterns=patterns)
    def augment(self, aux, /, *, columns, demand):
        # aux
        if not issubclass(type(aux), BASEData):
            aux = BASEData(aux)
        aux = aux.data
        auxrowss = _collections.defaultdict(list)
        for i, row in aux.iterrows():
            ID = tuple(row.get(col, "") for col in columns)
            auxrowss[ID].append(row)
        # table
        table = self.data
        rows = list()
        for i, row in table.iterrows():
            ID = tuple(row[col] for col in columns)
            if demand and (0 == len(auxrowss[ID])):
                continue
            try:
                rows.append(_utl.fuse(row, *auxrowss[ID]))
            except AssertionError as err:
                info = dict(zip(list(columns), list(ID)))
                raise KeyError(f"{info} cannot be unified: {err}")
        return BASEData(rows)
    def unify(self, *, columns, purge=False):
        cls = type(self)
        if columns is None:
            return cls(self)
        columns = self.columns(*columns)
        rdss = _collections.defaultdict(list)
        for i, row in self.data.iterrows():
            rd = row#.to_dict()
            ID = tuple(rd.get(col, "") for col in columns)
            rdss[ID].append(rd)
        if purge:
            ID = ("",) * len(columns)
            rdss.pop(ID, None)
        rows = list()
        for ID, rds in rdss.items():
            try:
                rows.append(_utl.fuse(*rds, reduce=True))
            except AssertionError as err:
                info = dict(zip(list(columns), list(ID)))
                _wng.warn(f"{info} cannot be unified: {err}")
        return cls(rows)
    def compress(self):
        return self.unify(
            table=table,
            columns=self.IDcolumns(),
            purge=True,
        )
    @classmethod
    def IDcolumns(cls):
        return list(cls._IDcolumns)
    @classmethod
    def basetype(cls):
        if cls._ext.startswith("."):
            raise ValueError()
        if cls._ext.startswith("base"):
            raise ValueError()
        return cls._ext[1:-4]
    @classmethod
    def from_file(cls, file, /):
        return super().from_file(file, ABASEData, CBASEData, DBASEData, MBASEData, YBASEData)
    @classmethod
    def concat(cls, *objs):
        objs = [cls(obj).data for obj in objs]
        ans = pd.concat(objs, axis=0, ignore_index=True)
        ans = cls(ans)
        return ans
    @classmethod
    def clone_data(cls, data, /):
        if type(data) is pd.DataFrame:
            table = data.copy()
        else:
            table = pd.DataFrame(data)
        table = table.applymap(_utl.string)
        cls._check(table)
        return table
    @classmethod
    def _check(cls, table):
        cls._check_BASE(table)
        if table.size == 0:
            return
        cls._check_type(table)
        cls._unique_constraint(table, columns=cls.IDcolumns())
    @staticmethod
    def _check_BASE(table):
        cols = list(table.columns)
        dc = list()
        for col in cols:
            assert col not in dc, f"Column name {ascii(col)} occures more than once! "
            dc.append(col)
            check_column(col)
        #table.applymap(_check_str)
        if 'GROUP' in table.columns:
            assert 'all' not in table['GROUP'].tolist()
        # plasmid name
        if 'PLASMID' in table.columns:
            for plasmid in table['PLASMID'].tolist():
                try:
                    assert type(float(plasmid)) is not float, "Improper plasmid name! "
                except ValueError:
                    continue
    @staticmethod
    def _check_type():
        pass
    @staticmethod
    def _unique_constraint(table, *, columns):
        if columns is None:
            return
        keys = list()
        duplicates = set()
        for i, row in self.data.iterrows():
            key = tuple(row[col] for col in columns)
            if key in keys:
                if key not in duplicates:
                    duplicates |= {key}
            else:
                keys.append(key)
        if len(duplicates):
            errors = [KeyError(f"The key {dict(zip(columns, key))} is not unique! ") for key in duplicates]
            raise GroupException(errors)
    def _deconstruct(self, *, patterns, outcolumn, purge):
        cls = type(self)
        table = self.data
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
                rz.append(_utl.fuse(rdA, rdB, rdC, reduce=True))
        ans = BASEData(rz)
        return ans    
    def _deconstruct_by_number(self, *, purge):
        dm = 1
        while 0 < len(self.columns(f"*_{dm}")):
            dm += 1
        dm -= 1
        patterns = dict()
        for n in range(1, dm + 1):
            patterns[str(n)] = f"*_{n}"
        ans = self._deconstruct(
            patterns=patterns, 
            outcolumn="",
            purge=purge,
        )
        return ans




class ABASEData(BASEData):
    _ext = ".abase"
    @staticmethod
    def _check_type(table):
        cols = list(table.columns)
        for IDcol in ('COLLECTION', 'NR'):
            assert IDcol in cols, f"IDcolumn '{IDcol}' is required! "
        assert 'CHAIN_TYPE' not in cols, ("Column 'CHAIN_TYPE' not allowed! ")
        assert _utl.any_columns(
            table, 
            patterns=["H_*", "K_*", "L_*"],
        ), "No chain-type-specific information found! "
        assert _utl.any_columns(
            table,
            patterns=['H_PCR2', 'H_PCR2_1', 'K_PCR2', 'K_PCR2_1', 'L_PCR2', 'L_PCR2_1'],
        ), "No chain-type-specific pcr2 found! "
        assert not _utl.any_columns(
            table, 
            patterns=['PCR2', 'PCR2_*'],
        ), "Not chain-type-specific pcr2-information found! "
        assert not _utl.any_columns(
            table, 
            patterns=['PLASMID', 'PLASMID_*'],
        ), "No plasmid-information allowed! "
    @property
    def dBASE(self):
        """convert aTable to dTable"""
        table = self._deconstruct_by_number(purge=False)
        CG = dict()
        for ct in "HKL":
            CG[f"{ct}C"] = f"{ct}_*"
        ans = table._deconstruct(
            patterns=CG, 
            outcolumn='CHAIN_TYPE',
            purge=False,
        )
        ans = DBASEData(ans)
        return ans






class CBASEData(BASEData):
    _ext = ".cbase"
    _IDcolumns = ('PLASMID',)
    @staticmethod
    def _check_type(table):
        cols = list(table.columns)
        assert ('PCR2' in cols), _pcr2_missing
        assert 'PLASMID' in cols, _plasmid_missing
        for i, row in table.iterrows():
            assert row.get('CHAIN_TYPE', "") in ["", 'HC', 'KC', 'LC', 'N/A'], f"error: {row.to_dict()} "







class DBASEData(BASEData):
    _ext = ".dbase"
    _IDcolumns = ('PCR2',)
    @classmethod
    def _check_type(cls, table):
        cols = list(table.columns)
        _ct_illegal = ("Illegal value in column 'CHAIN_TYPE'! ")


        assert ('PCR2' in cols), _pcr2_missing
        for IDcol in ('COLLECTION', 'NR'):
            assert IDcol in cols, f"IDcolumn '{IDcol}' is required! "
        assert 'CHAIN_TYPE' in cols, "Column 'CHAIN_TYPE' is required! "
        assert 'PCR2' in cols, "Column 'PCR2' is required! "
        assert all(table['CHAIN_TYPE'].isin(['HC', 'KC', 'LC', 'N/A'])), _ct_illegal
        if 'CN' in cols:
            assert all(
                (table['CHAIN_TYPE'] != 'HC') | (table['CN'] == "")
            ), "Heavy chains are not allowed to have CN-values! "




class MBASEData(BASEData):
    _ext = ".mbase"
    _IDcolumns = ('COLLECTION', 'NR', 'LIGHT_CHAIN_TYPE')
    @classmethod
    def _check_type(cls, table):
        cols = list(table.columns)
        for IDcol in cls.IDcolumns():
            assert IDcol in cols, f"The column '{IDcol}' is required! "
        assert 'CHAIN_TYPE' not in cols, ("Illegal column 'CHAIN_TYPE'! ")
        for CT in table['LIGHT_CHAIN_TYPE']:
            assert (
                CT in ["", 'KC', 'LC']
            ), f"Illegal value {ascii(CT)} in column 'LIGHT_CHAIN_TYPE'! "


class YBASEData(BASEData):
    _ext = ".ybase"
    @staticmethod
    def _check_type(table):
        cols = list(table.columns)
        assert ('PCR2' in cols), _pcr2_missing
        assert _utl.any_columns(
            table, 
            patterns=['PLASMID', 'PLASMID_1'],
        ), _plasmid_missing
        n = 1
        while f"PLASMID_{n}" in table.columns:
            for plasmid in table[f"PLASMID_{n}"].tolist():
                try:
                    assert type(float(plasmid)) is not float, "Improper plasmid name! "
                except ValueError:
                    continue
            n += 1
        if 'CHAIN_TYPE' in cols:
            assert all(table['CHAIN_TYPE'].isin(["", 'HC', 'KC', 'LC', 'N/A'])), _ct_illegal
    @property
    def cBASE(self):
        return CBASEData(self._deconstruct_by_number())
