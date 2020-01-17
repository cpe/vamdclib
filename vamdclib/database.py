# -*- coding: utf-8 -*-
"""
This module contains functionality to create a sqlite3 database and to store
spectroscopic data in it. The data is retrieved via queries to VAMDC database
nodes. Methods to insert and update the data are included. The data model of
the sqlite3 database is fixed and adaptations to other needs require changes to
the code within this module.
"""
from __future__ import print_function

import sys
import sqlite3
from datetime import datetime
from dateutil import parser

if sys.version_info[0] == 3:
    from . import functions
    from . import request as r
    from . import nodes
    from . import specmodel
    from . import settings
else:
    import functions
    import request as r
    import nodes
    import specmodel
    import settings

# List of Temperatures for which the Partitionfunction is stored in the sqlite
# database.
TEMPERATURES = [1.072, 1.148, 1.230, 1.318, 1.413, 1.514, 1.622, 1.738, 1.862,
                1.995, 2.138, 2.291, 2.455, 2.630, 2.725, 2.818, 3.020, 3.236,
                3.467, 3.715, 3.981, 4.266, 4.571, 4.898, 5.000, 5.248, 5.623,
                6.026, 6.457, 6.918, 7.413, 7.943, 8.511, 9.120, 9.375, 9.772,
                10.471, 11.220, 12.023, 12.882, 13.804, 14.791, 15.849, 16.982,
                18.197, 18.750, 19.498, 20.893, 22.387, 23.988, 25.704, 27.542,
                29.512, 31.623, 33.884, 36.308, 37.500, 38.905, 41.687, 44.668,
                47.863, 51.286, 54.954, 58.884, 63.096, 67.608, 72.444, 75.000,
                77.625, 83.176, 89.125, 95.499, 102.329, 109.648, 117.490,
                125.893, 134.896, 144.544, 150.000, 154.882, 165.959, 177.828,
                190.546, 204.174, 218.776, 225.000, 234.423, 251.189, 269.153,
                288.403, 300.000, 309.030, 331.131, 354.813, 380.189, 407.380,
                436.516, 467.735, 500.000, 501.187, 537.032, 575.440, 616.595,
                660.693, 707.946, 758.578, 812.831, 870.964, 933.254, 1000.000]

# string with field names for partition functions
SQL_TEMP_FIELDS = ",".join(("PF_%.3f" % T
                            for T in TEMPERATURES)).replace('.', '_')

# Possible stati of database entries (Partition functions). This information is
# used to control the update mechanism.
#
STATI = ['New',  # indicates that this entry is available.
         'Update Available',  # indicates that an update is available
         'Up-To-Date',  # entry is up-to-date
         'Outdated',  # indicates that the entry is outdated
         'Keep',  # keep this entry, even if there are updates available
         'Updating',  # an update is in progress
         'Update Failed',  # Update is available, but last attempt failed.
         ]

URL_STRING = "sync?LANG=VSS2&amp;REQUEST=doQuery&amp;"\
             "FORMAT=XSAMS&amp;QUERY=Select+*+where+SpeciesID%3D"


def make_name(name, nsi=None, hfs=None, state=None):
    """
    Generate a name that is easy to understand by users:

    (name);(nsi);(state);(hfs)
    """
    if nsi is not None and nsi != '':
        name += ";%s" % nsi
    if state is not None and state != '':
        name += ";%s" % state
    if hfs is not None and hfs != '':
        name += ";%s" % hfs

    return name


def parse_name(name, delimiter=" "):
    # remove delimiter if it is attached at the end
    name = name.strip()
    if name[-1] == delimiter:
        name = name[:-1]

    data = name.split(delimiter)
    # first field is always the name
    name = data.pop(0).strip()
    nsi = None
    state = None
    hfs = None
    elecstate = None

    # hyperfine info is attached at the end
    while len(data) > 0:
        field = data.pop(-1).strip()
        if field[:3] == 'hyp':
            hfs = field
        elif 'v' in field:
            state = field
        elif (field == 'ortho'
              or field == 'para'
              or field == 'A'
              or field == 'E'):
            nsi = field
        else:
            elecstate = field

    return name, elecstate, nsi, state, hfs


class PFrow(object):
    """
    """
    def __init__(self, **kwds):
        self.id = None
        self.name = None
        self.species_id = None
        self.vamdc_species_id = None
        self.stoichiometricformula = None
        self.ordinarystructuralformula = None
        self.chemicalname = None
        self.hfs = None
        self.nsi = None
        self.vibstate = None
        self.comment = None
        self.recommendation = None
        self.uuid = None
        self.doi = None
        self.resource_id = None
        self.url = None
        self.status = None
        self.createdate = None
        self.checkdate = None

        # Push any keywords to attributes of this instance
        self.update(**kwds)

    def update(self, **kwds):
        """
        Update the instance with a dictionary containing its new
        properties.
        """
        for ck in kwds.keys():
            if ck in vars(self).keys():
                self.__dict__.update({ck: kwds[ck]})
            else:
                setattr(self, ck, kwds[ck])

    def __str__(self):
        return ("%7d %12s %-20s %-40s %-40s %-30s %20s %5s %10s%10s%4s"
                % (self.id,
                   self.species_id,
                   self.name,
                   self.stoichiometricformula,
                   self.ordinarystructuralformula,
                   self.chemicalname,
                   self.vibstate,
                   self.elecstate,
                   self.hfs,
                   self.nsi,
                   self.recommendation))

    def __repr__(self):
        return self.__str__()


class TransitionRow(object):
    """
    Database entry for transition table.
    """
    def __init__(self, **kwds):
        # Id of the corresponding entry in partition functions
        self.pf_id = None
        self.name = None
        self.frequency = None
        self.intensity = None
        self.einsteinA = None
        self.oscillatorstrength = None
        self.uncertainty = None
        self.energyLower = None
        self.upperStateDegeneracy = None
        self.nuclearSpinIsomer = None
        self.hfs = None
        self.case = None
        self.upperStateQuantumNumbers = None
        self.lowerStateQuantumNumbers = None

        # Push any keywords to attributes of this instance
        self.update(**kwds)

    def update(self, **kwds):
        """
        Update the instance with a dictionary containing its new
        properties.
        """
        for ck in kwds.keys():
            if ck in vars(self).keys():
                self.__dict__.update({ck: kwds[ck]})
            else:
                setattr(self, ck, kwds[ck])

    def __str__(self):
        return ("%7d%7d%20s%15.4lf%10.4lf %10.5g %-70s%-70s"
                % (self.id,
                   self.pf_id,
                   self.name,
                   self.frequency,
                   self.uncertainty,
                   self.einsteinA,
                   self.upperStateQuantumNumbers,
                   self.lowerStateQuantumNumbers))

    def __repr__(self):
        return self.__str__()


class Transitions(list):

    def __init__(self, **kwds):
        super(Transitions, self).__init__()
        self.__filter__(**kwds)

    def objects(self, **kwds):
        return self

    def __filter__(self, **kwds):
        # remove old data
        # self.__init__()
        sql_where = []
        sql_fields = []

        if 'upper_state_quantumnumbers' in kwds:
            usq = kwds.pop('upper_state_quantumnumbers')
            if '%' in usq:
                sql_where.append("T_UpperStateQuantumNumbers like ? ")
                sql_fields.append(usq)
            else:
                sql_where.append("T_UpperStateQuantumNumbers = ? ")
                sql_fields.append(usq)

        if 'lower_state_quantumnumbers' in kwds:
            lsq = kwds.pop('lower_state_quantumnumbers')
            if '%' in lsq:
                sql_where.append("T_LowerStateQuantumNumbers like ? ")
                sql_fields.append(lsq)
            else:
                sql_where.append("T_LowerStateQuantumNumbers = ? ")
                sql_fields.append(lsq)

        sdb = Database()
        # ids = sdb.filter_species(**kwds)
        ids = Species(**kwds).ids()
        sql_fields = ids + sql_fields

        sql = ("SELECT T_ID, T_PF_ID, T_Name, T_Frequency, "
               "       T_Intensity, T_EinsteinA, T_OscillatorStrength, "
               "       T_Uncertainty, T_EnergyLower, T_UpperStateDegeneracy, "
               "       T_NuclearSpinIsomer, T_HFS, T_Case, "
               "       T_UpperStateQuantumNumbers, "
               "       T_LowerStateQuantumNumbers "
               "       FROM Transitions WHERE T_PF_ID in (%s)"
               % ','.join('?'*len(ids)))

        if len(sql_fields) > len(ids):
            sql_where = ' AND ' + ' AND '.join(sql_where)
        else:
            sql_where = ''

        cursor = sdb.conn.cursor()
        cursor.execute(sql + sql_where, sql_fields)

        rows = cursor.fetchall()
        for row in rows:
            t = TransitionRow()
            t.id = row[0]
            t.pf_id = row[1]
            t.name = row[2]
            t.frequency = row[3]
            t.intensity = row[4]
            t.einsteinA = row[5]
            t.oscillatorstrength = row[6]
            t.uncertainty = row[7]
            t.energyLower = row[8]
            t.upperStateDegeneracy = row[9]
            t.nuclearSpinIsomer = row[10]
            t.hfs = row[11]
            t.case = row[12]
            t.upperStateQuantumNumbers = row[13]
            t.lowerStateQuantumNumbers = row[14]

            self.append(t)

    def __str__(self):
        return '\n'.join([i.__str__() for i in self])


class Species(list):

    def __init__(self, **kwds):
        super(Species, self).__init__()
        self.__filter__(**kwds)

    def ids(self):
        return [s.id for s in self]

    def objects(self, **kwds):
        return self

    def __filter__(self, **kwds):
        """
        Returns list of database ids for rows in Partitionfunction-Table
        that match the current filter.

        Valid keywords are:
            vamdcspecies_id
            species_id
            stoichiometric_formula
            structural_formula
            hfs_level
            vibstate
            nsi
            recommended
            name
        """
        # remove old data
        # self.__init__()

        sdb = Database()

        filter_opts = ['vamdcspecies_id',
                       'species_id',
                       'stoichiometric_formula',
                       'structural_formula',
                       'hfs_level',
                       'vibstate',
                       'elecstate',
                       'nsi',
                       'name',
                       'recommended']
        if 'name' in kwds:
            structural_formula, elecstate, nsi, state, hfs = \
                    parse_name(kwds.get('name'), delimiter=';')
            if structural_formula is not None \
                    and 'structural_formula' not in kwds:
                kwds['structural_formula'] = structural_formula
            if nsi is not None and 'nsi' not in kwds:
                kwds['nsi'] = nsi
            if state is not None and 'vibstate' not in kwds:
                kwds['vibstate'] = state
            if hfs is not None and 'hfs_level' not in kwds:
                kwds['hfs_level'] = hfs
            if elecstate is not None and 'elecstate' not in kwds:
                kwds['elecstate'] = elecstate

        for k in kwds:
            if k not in filter_opts:
                raise TypeError("'%s' is an invalid keyword "
                                "argument for this function!\n"
                                "Valid keywords are: %s "
                                % (k, ",".join(filter_opts))
                                )
            elif k == 'vibstate':
                kwds['vibstate'] = kwds['vibstate'].replace(' ', '')
            elif k == 'elecstate':
                kwds['elecstate'] = kwds['elecstate'].replace(' ', '')

        cursor = sdb.conn.cursor()
        sql = ("SELECT PF_ID, PF_Name, PF_VamdcSpeciesID, "
               "  PF_SpeciesID, PF_StoichiometricFormula, "
               "  PF_OrdinaryStructuralFormula, PF_ChemicalName, "
               "  PF_NuclearSpinIsomer, PF_HFS, PF_VibState, "
               "  PF_ElecState, PF_ResourceID, PF_URL, PF_Comment, "
               "  PF_Recommendation, PF_UUID, PF_DOI, PF_Status, "
               "  PF_Createdate, PF_Checkdate "
               " FROM Partitionfunctions ")
        sql_where = []
        sql_fields = []
        if kwds.get('vamdcspecies_id') is not None:
            sql_where.append("PF_VamdcSpeciesID = ?")
            sql_fields.append(kwds.get('vamdcspecies_id'))

        if kwds.get('species_id') is not None:
            sql_where.append("PF_SpeciesID = ?")
            sql_fields.append(kwds.get('species_id'))

        if kwds.get('stoichiometric_formula') is not None:
            if '%' in kwds.get('stoichiometric_formula'):
                sql_where.append("PF_StoichiometricFormula like ?")
                sql_fields.append(kwds.get('stoichiometric_formula'))
            else:
                sql_where.append("PF_StoichiometricFormula = ?")
                sql_fields.append(kwds.get('stoichiometric_formula'))

        if kwds.get('structural_formula') is not None:
            if '%' in kwds.get('structural_formula'):
                sql_where.append("PF_OrdinaryStructuralFormula like ?")
                sql_fields.append(kwds.get('structural_formula'))
            else:
                sql_where.append("PF_OrdinaryStructuralFormula = ?")
                sql_fields.append(kwds.get('structural_formula'))
        if kwds.get('hfs_level') is not None:
            if kwds.get('hfs_level') == 0 or kwds.get('hfs_level') == 'hyp0':
                sql_where.append("PF_HFS is NULL")
            else:
                hfs = kwds.get('hfs_level')
                if not type(hfs) == str:
                    hfs = 'hyp%d' % hfs
                sql_where.append("PF_HFS = ?")
                sql_fields.append(hfs)

        if kwds.get('vibstate') is not None:
            if '%' in kwds.get('vibstate'):
                sql_where.append("replace(PF_VibState,' ','') like ?")
                sql_fields.append(kwds.get('vibstate'))
            else:
                sql_where.append("replace(PF_VibState,' ','') = ?")
                sql_fields.append(kwds.get('vibstate'))

        if kwds.get('elecstate') is not None:
            if '%' in kwds.get('elecstate'):
                sql_where.append("replace(PF_ElecState,' ','') like ?")
                sql_fields.append(kwds.get('elecstate'))
            else:
                sql_where.append("replace(PF_ElecState,' ','') = ?")
                sql_fields.append(kwds.get('elecstate'))

        if kwds.get('nsi') is not None:
            sql_where.append("PF_NuclearSpinIsomer = ?")
            sql_fields.append(kwds.get('nsi'))

        if kwds.get('recommended') is not None and kwds.get('recommended'):
            sql_where.append("PF_Recommendation = 1")

        sql_where = ' AND '.join(sql_where)

        cursor.execute(sql + 'WHERE ' + sql_where, sql_fields)
        rows = cursor.fetchall()

        for row in rows:
            s = PFrow()
            s.id = row[0]
            s.name = row[1]
            s.species_id = row[3]
            s.vamdc_species_id = row[2]
            s.stoichiometricformula = row[4]
            s.ordinarystructuralformula = row[5]
            s.chemicalname = row[6]
            s.nsi = row[7]
            s.hfs = row[8]
            s.vibstate = row[9]
            s.elecstate = row[10]
            s.resource_id = row[11]
            s.url = row[12]
            s.comment = row[13]
            s.recommendation = row[14]
            s.uuid = row[15]
            s.doi = row[16]
            s.status = row[17]
            s.createdate = row[18]
            s.checkdate = row[19]

            self.append(s)

    def __str__(self):
        return '\n'.join([i.__str__() for i in self])


class Database(object):
    """
    An instance of Database contains methods to store data obtained from VAMDC
    nodes in an sqlite database.

    :ivar sqlite3.Connection conn: connection handler to the sqlite database
    """
    def __init__(self, database_file=settings.DATABASE_FILE):
        """
        The connection to the sqlite3 database is established during
        initialization of the Database-Instance. A new database will be created
        if it does not exist.

        :ivar str database_file: Path to the sqlite3 database file. The value
                                 given in the settings.py - file will be used
                                 as default.
        """
        try:
            self.conn = sqlite3.connect(database_file)
        except sqlite3.Error as e:
            print(" ")
            print("Can not connect to sqlite3 databse %s." % database_file)
            print("Error: %d: %s" % (e.args[0], e.args[1]))
        return

    def create_structure(self):
        """
        Creates tables in the sqlite3 database if they do not exist. The
        database  layout allows to store transition frequencies and partition
        functions according to the needs for astrochemical modeling.

        Tables which will be created:
        - Partitionfunctions
        - Transitions
        """

        cursor = self.conn.cursor()
        # drop tables if they exist
        stmts = ("DROP TABLE IF EXISTS Partitionfunctions;",
                 "DROP TABLE IF EXISTS Transitions;",)

        for stmt in stmts:
            cursor.execute(stmt)

        # insert transitions
        sql_create_transitions = """CREATE TABLE Transitions (
        T_ID INTEGER primary key,
        T_PF_ID INTEGER REFERENCES Partitionfunctions,
        T_Name TEXT,
        T_Frequency REAL,
        T_Intensity REAL,
        T_EinsteinA REAL,
        T_OscillatorStrength REAL,
        T_Uncertainty REAL,
        T_EnergyLower REAL,
        T_UpperStateDegeneracy INTEGER,
        T_NuclearSpinIsomer TEXT,
        T_HFS TEXT,
        T_Case TEXT,
        T_UpperStateQuantumNumbers TEXT,
        T_LowerStateQuantumNumbers TEXT) """

        sql_create_partitionfunctions = """ CREATE TABLE Partitionfunctions (
        PF_ID INTEGER primary key,
        PF_Name TEXT,
        PF_VamdcSpeciesID TEXT,
        PF_SpeciesID TEXT,
        PF_StoichiometricFormula TEXT,
        PF_OrdinaryStructuralFormula TEXT,
        PF_ChemicalName TEXT,
        PF_NuclearSpinIsomer TEXT,
        PF_HFS TEXT,
        PF_VibState TEXT,
        PF_ElecState TEXT,
        PF_1_072 REAL,
        PF_1_148 REAL,
        PF_1_230 REAL,
        PF_1_318 REAL,
        PF_1_413 REAL,
        PF_1_514 REAL,
        PF_1_622 REAL,
        PF_1_738 REAL,
        PF_1_862 REAL,
        PF_1_995 REAL,
        PF_2_138 REAL,
        PF_2_291 REAL,
        PF_2_455 REAL,
        PF_2_630 REAL,
        PF_2_725 REAL,
        PF_2_818 REAL,
        PF_3_020 REAL,
        PF_3_236 REAL,
        PF_3_467 REAL,
        PF_3_715 REAL,
        PF_3_981 REAL,
        PF_4_266 REAL,
        PF_4_571 REAL,
        PF_4_898 REAL,
        PF_5_000 REAL,
        PF_5_248 REAL,
        PF_5_623 REAL,
        PF_6_026 REAL,
        PF_6_457 REAL,
        PF_6_918 REAL,
        PF_7_413 REAL,
        PF_7_943 REAL,
        PF_8_511 REAL,
        PF_9_120 REAL,
        PF_9_375 REAL,
        PF_9_772 REAL,
        PF_10_471 REAL,
        PF_11_220 REAL,
        PF_12_023 REAL,
        PF_12_882 REAL,
        PF_13_804 REAL,
        PF_14_791 REAL,
        PF_15_849 REAL,
        PF_16_982 REAL,
        PF_18_197 REAL,
        PF_18_750 REAL,
        PF_19_498 REAL,
        PF_20_893 REAL,
        PF_22_387 REAL,
        PF_23_988 REAL,
        PF_25_704 REAL,
        PF_27_542 REAL,
        PF_29_512 REAL,
        PF_31_623 REAL,
        PF_33_884 REAL,
        PF_36_308 REAL,
        PF_37_500 REAL,
        PF_38_905 REAL,
        PF_41_687 REAL,
        PF_44_668 REAL,
        PF_47_863 REAL,
        PF_51_286 REAL,
        PF_54_954 REAL,
        PF_58_884 REAL,
        PF_63_096 REAL,
        PF_67_608 REAL,
        PF_72_444 REAL,
        PF_75_000 REAL,
        PF_77_625 REAL,
        PF_83_176 REAL,
        PF_89_125 REAL,
        PF_95_499 REAL,
        PF_102_329 REAL,
        PF_109_648 REAL,
        PF_117_490 REAL,
        PF_125_893 REAL,
        PF_134_896 REAL,
        PF_144_544 REAL,
        PF_150_000 REAL,
        PF_154_882 REAL,
        PF_165_959 REAL,
        PF_177_828 REAL,
        PF_190_546 REAL,
        PF_204_174 REAL,
        PF_218_776 REAL,
        PF_225_000 REAL,
        PF_234_423 REAL,
        PF_251_189 REAL,
        PF_269_153 REAL,
        PF_288_403 REAL,
        PF_300_000 REAL,
        PF_309_030 REAL,
        PF_331_131 REAL,
        PF_354_813 REAL,
        PF_380_189 REAL,
        PF_407_380 REAL,
        PF_436_516 REAL,
        PF_467_735 REAL,
        PF_500_000 REAL,
        PF_501_187 REAL,
        PF_537_032 REAL,
        PF_575_440 REAL,
        PF_616_595 REAL,
        PF_660_693 REAL,
        PF_707_946 REAL,
        PF_758_578 REAL,
        PF_812_831 REAL,
        PF_870_964 REAL,
        PF_933_254 REAL,
        PF_1000_000 REAL,
        PF_ResourceID TEXT,
        PF_URL TEXT,
        PF_Comment TEXT,
        PF_Recommendation TEXT,
        PF_UUID TEXT,
        PF_DOI TEXT,
        PF_Status TEXT,
        PF_Createdate,
        PF_Checkdate)"""

        sql_create_idx_pf_id = \
            "CREATE INDEX 'IDX_PF_ID' ON Transitions (T_PF_ID);"
        sql_create_idx_pfname = \
            "CREATE INDEX 'IDX_PF_Name' ON Partitionfunctions (PF_Name);"
        sql_create_idx_tname = \
            "CREATE INDEX 'IDX_T_Name' ON Transitions "\
            "(T_Name, T_Frequency, T_EnergyLower);"
        sql_create_idx_freq = \
            "CREATE INDEX 'IDX_T_Frequency' ON Transitions "\
            "(T_Frequency, T_EnergyLower);"

        cursor.execute(sql_create_partitionfunctions)
        cursor.execute(sql_create_transitions)
        cursor.execute(sql_create_idx_pf_id)
        cursor.execute(sql_create_idx_pfname)
        cursor.execute(sql_create_idx_tname)
        cursor.execute(sql_create_idx_freq)

        self.conn.commit()

        return

    def db_insert_partitionfunction(self, pfrow, commit=True):
        """
        Inserts the row into the database table partitionfunction

        Returns its rowid
        """
        cursor = self.conn.cursor()

        cursor.execute("""INSERT INTO Partitionfunctions
                         (PF_Name,
                          PF_SpeciesID,
                          PF_VamdcSpeciesID,
                          PF_StoichiometricFormula,
                          PF_OrdinaryStructuralFormula,
                          PF_ChemicalName,
                          PF_HFS,
                          PF_NuclearSpinIsomer,
                          PF_VibState,
                          PF_Recommendation,
                          PF_Comment,
                          PF_ResourceID,
                          PF_URL,
                          PF_Status,
                          PF_Createdate,
                          PF_Checkdate)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                       (pfrow.name,
                        pfrow.species_id,
                        pfrow.vamdc_species_id,
                        pfrow.stoichiometricformula,
                        pfrow.ordinarystructuralformula,
                        pfrow.chemicalname,
                        pfrow.hfs,
                        pfrow.nsi,
                        pfrow.vibstate,
                        pfrow.recommendation,
                        pfrow.comment,
                        pfrow.resource_id,
                        pfrow.url,
                        'new',
                        datetime.now(),
                        datetime.now()))

        if commit:
            self.conn.commit()

        cursor.execute("SELECT last_insert_rowid();")
        rowid = cursor.fetchone()[0]

        cursor.close()

        return rowid

    def doublicate_pf_entry_for_hfs(self,
                                    species_id,
                                    name,
                                    nsi,
                                    state,
                                    hfs,
                                    elecstate,
                                    ):
        """
        Doublicate an entry to store information for hyperfine transitions.
        Partitionfunctions are the same if species-id and nuclear spin isomers
        are the same.
        """
        cursor = self.conn.cursor()

        cursor.execute("""INSERT INTO Partitionfunctions
                         (PF_Name,
                          PF_SpeciesID,
                          PF_VamdcSpeciesID,
                          PF_StoichiometricFormula,
                          PF_OrdinaryStructuralFormula,
                          PF_ChemicalName,
                          PF_HFS,
                          PF_NuclearSpinIsomer,
                          PF_VibState,
                          PF_ElecState,
                          PF_Recommendation,
                          PF_Comment,
                          PF_ResourceID,
                          PF_URL,
                          PF_Status,
                          {fieldnames},
                          PF_Createdate,
                          PF_Checkdate)
                        SELECT
                           PF_Name,
                           PF_SpeciesID,
                           PF_VamdcSpeciesID,
                           PF_StoichiometricFormula,
                           PF_OrdinaryStructuralFormula,
                           PF_ChemicalName,
                           ?,
                           PF_NuclearSpinIsomer,
                           ?,
                           ?,
                           PF_Recommendation,
                           PF_Comment,
                           PF_ResourceID,
                           PF_URL,
                           PF_Status,
                           {fieldnames},
                           PF_Createdate,
                           PF_Checkdate
                        FROM Partitionfunctions
                        WHERE PF_SpeciesID=?
                         AND IFNULL(PF_NuclearSpinIsomer,'')=IFNULL(?,'')
                        LIMIT 1
                        """.format(fieldnames=SQL_TEMP_FIELDS),
                       (hfs, state, elecstate, species_id, nsi))

        self.conn.commit()
        db_id = cursor.lastrowid
        cursor.close()
        return db_id

    def get_id_by_name(self, name):
        """
        Get database id from the given name.
        """

    def filter_species(self, **kwds):
        """
        Returns list of database ids for rows in Partitionfunction-Table
        that match the current filter.

        Valid keywords are:
            vamdcspecies_id
            species_id
            stoichiometric_formula
            structural_formula
            hfs_level
            vibstate
        """
        ids = []

        filter_opts = ['vamdcspecies_id',
                       'species_id',
                       'stoichiometric_formula',
                       'structural_formula',
                       'hfs_level',
                       'vibstate']
        for k in kwds:
            if k not in filter_opts:
                raise TypeError("'%s' is an invalid keyword "
                                "argument for this function" % k)

        cursor = self.conn.cursor()
        sql = "SELECT PF_ID FROM Partitionfunctions "
        sql_where = []
        sql_fields = []
        if kwds.get('vamdcspecies_id') is not None:
            sql_where.append("PF_VamdcSpeciesID = ?")
            sql_fields.append(kwds.get('vamdcspecies_id'))

        if kwds.get('species_id') is not None:
            sql_where.append("PF_SpeciesID = ?")
            sql_fields.append(kwds.get('species_id'))

        if kwds.get('stoichiometric_formula') is not None:
            if '%' in kwds.get('stoichiometric_formula'):
                sql_where.append("PF_StoichiometricFormula like ?")
                sql_fields.append(kwds.get('stoichiometric_formula'))
            else:
                sql_where.append("PF_StoichiometricFormula = ?")
                sql_fields.append(kwds.get('stoichiometric_formula'))

        if kwds.get('structural_formula') is not None:
            if '%' in kwds.get('structural_formula'):
                sql_where.append("PF_OrdinaryStructuralFormula like ?")
                sql_fields.append(kwds.get('structural_formula'))
            else:
                sql_where.append("PF_OrdinaryStructuralFormula = ?")
                sql_fields.append(kwds.get('structural_formula'))
        if kwds.get('hfs_level') is not None:
            if kwds.get('hfs_level') == 0:
                sql_where.append("PF_HFS is NULL")
            else:
                sql_where.append("PF_HFS = ?")
                sql_fields.append(kwds.get('hfs_level'))

        if kwds.get('vibstate') is not None:
            sql_where.append("PF_VibState = ?")
            sql_fields.append(kwds.get('vibstate'))

        sql_where = ' AND '.join(sql_where)

        cursor.execute(sql + 'WHERE ' + sql_where, sql_fields)
        rows = cursor.fetchall()
        for row in rows:
            ids.append(row[0])

        return ids

    def set_status(self, species_id, status, db_id=None):
        """
        Sets status of an entry in the sqlite-db.
        If db_id is not None than only the specific row in
        the sqlite database will be updated.

        :param species_id: Species-id from original database
        :param status: new status to be set
        :param db_id: id (entry) in sqlite database
        """

        if status not in STATI:
            print("%s is not a valid status!")
            return

        cursor = self.conn.cursor()
        if db_id is not None:

            cursor.execute("UPDATE PartitionFunctions "
                           "SET PF_Status = ? ,"
                           "PF_Checkdate = ? "
                           "WHERE PF_ID = ? ",
                           (status, datetime.now(), db_id))
        else:
            cursor.execute("UPDATE PartitionFunctions "
                           "SET PF_Status = ? ,"
                           "PF_Checkdate = ? "
                           "WHERE PF_SpeciesID = ? ",
                           (status, datetime.now(), species_id))
        self.conn.commit()
        cursor.close()

    def set_uuid(self, id, uuid):
        """
        Inserts the UUID (query identifier)

        :param id: id of the entry in partitionfunctions
        :type id: int
        :param uuid: Identifier of the database query
        :type uuid: str
        """
        if uuid is None:
            return
        cursor = self.conn.cursor()
        cursor.execute("UPDATE PartitionFunctions "
                       "SET PF_UUID = ? "
                       "WHERE PF_ID = ? ",
                       (uuid, id))
        self.conn.commit()
        cursor.close()

    def update_pf_state(self, id, state, hfs, elecstate, commit=True):
        """
        Updates state and hyperfine structure information
        for new entries.
        """
        cursor = self.conn.cursor()
        cursor.execute("UPDATE PartitionFunctions "
                       "SET PF_VibState = ?, "
                       "    PF_HFS = ? , "
                       "    PF_ElecState = ? "
                       "WHERE PF_ID = ? ",
                       (state, hfs, elecstate, id))
        if commit:
            self.conn.commit()
        cursor.close()

    def check_for_updates(self, node=None):
        """
        Checks for each database entry if an update for the molecular or atomic
        specie is available in the specified VAMDC database node.
        Only head-requests are performed and only the last-modified date will
        be retrieved. Updates have to be called separately.

        :ivar nodes.Node node: only this VAMDC database node which will be
                               checked for updates
        """
        count_updates = 0
        counter = 0
        nl = nodes.Nodelist()

        cursor = self.conn.cursor()
        if node is None:
            cursor.execute("SELECT PF_Name, PF_SpeciesID, PF_VamdcSpeciesID, "
                           "datetime(PF_Checkdate), PF_Status, PF_ResourceID "
                           "FROM Partitionfunctions ")
        else:
            cursor.execute("SELECT PF_Name, PF_SpeciesID, PF_VamdcSpeciesID, "
                           "datetime(PF_Checkdate), PF_Status, PF_ResourceID "
                           "FROM Partitionfunctions "
                           "WHERE PF_ResourceID = ?", (node.identifier))

        rows = cursor.fetchall()
        num_rows = len(rows)
        request = r.Request()

        for row in rows:
            counter += 1
            print("%5d/%5d: Check specie %-55s (%-15s): "
                  % (counter, num_rows, row[0], row[1]), end=' ')
            species_id = row[1]
            try:
                node = nl.getnode(row[5])
            except Exception:
                print("Resource %s not found!" % row[5])
                print("Maybe the resource identifier has changed!")
                print("See VAMDC registry: https://registry.vamdc.eu")
                continue

            species_id_int = int(row[1].split('-')[-1])
            query_string = "SELECT ALL WHERE SpeciesID=%s" % species_id_int
            request.setquery(query_string)
            request.setnode(node)

            try:
                changedate = request.getlastmodified()
            except r.TimeOutError:
                print("TIMEOUT")
                continue
            except r.NoContentError:
                print("ENTRY OUTDATED")
                self.set_status(species_id, 'Outdated')
                changedate = None
                continue
            except Exception as e:
                print("Error in getlastmodified: %s " % str(e))
                print("Status - code: %s" % str(request.status))
                changedate = None
                continue

            tstamp = parser.parse(row[3] + " GMT")
            status = row[4]

            if changedate is None:
                print(" -- UNKNOWN (Could not retrieve information)")
                continue
            if tstamp < changedate or status == 'Update Available':
                print(" -- UPDATE AVAILABLE ")
                self.set_status(species_id, 'Update Available')
                count_updates += 1
            elif status == 'Up-To-Date':
                print(" -- up to date")
                self.set_status(species_id, 'Up-To-Date')
            else:
                # reset status (maybe update process was interupted
                print("Status changed from %s to 'Update Available'" %
                      status)
                self.set_status(species_id, 'Update Available')
                count_updates += 1

        if count_updates == 0:
            print("\r No updates for your entries available")
        print("Done")
        print("Updates for %d species are available.\n\n"
              "Run update_species_data() to update their transitions!\n\n"
              % count_updates)

    def check_for_new_species(self, node):
        """
        Checks for new entries in the VAMDC database node which are not
        available in the local sqlite3 database.

        :ivar nodes.Node node: VAMDC database node which will be checked
                               for updates
        """

        count_species_imported = 0
        species_import_failed = []
        cursor = self.conn.cursor()

        # Try to identify node if only specified by a string
        if type(node) == str:
            nl = nodes.Nodelist()
            node = nl.findnode(node)

        species = r.getspecies(node=node)

        print("----------------------------------------------------------")
        print("Query '{dbname}' for new species ".format(dbname=node.name))
        print("----------------------------------------------------------")

        for species_id in set(species['Molecules']) \
                | set(species['Atoms']):
            try:
                cursor.execute("SELECT PF_ID FROM Partitionfunctions "
                               "WHERE PF_SpeciesID=?",
                               [(species_id)])
                exist = cursor.fetchone()
                if exist is None:
                    pfrow = PFrow(species_id=species_id)
                    pfrow.resource_id = str(node.identifier)

                    if species_id in species['Atoms'].keys():
                        # print("ID: %s" % species['Atoms'][species_id])
                        name = self.createatomname(
                            species['Atoms'][species_id])
                        pfrow.name = make_name(name)
                        pfrow.stoichiometricformula = name
                        pfrow.ordinarystructuralformula = name
                        pfrow.chemicalname = name

                        if 'Comment' not in \
                           species['Atoms'][species_id].__dict__:
                            pfrow.comment = ""
                        else:
                            pfrow.comment = \
                                    species['Atoms'][species_id].Comment

                        pfrow.vamdc_species_id = (
                            "%s" %
                            (species['Atoms'][species_id].VAMDCSpeciesID))

                        # insert new row into db (status='new')
                        id = self.db_insert_partitionfunction(pfrow,
                                                              commit=False)
                        self.conn.commit()
                        print("Imported new entry %s (ID: %s)!"
                              % (pfrow.name, species_id))
                        count_species_imported += 1

                    else:
                        # print("ID: %s" % species['Molecules'][species_id])
                        formula = (species['Molecules'][species_id]
                                   .OrdinaryStructuralFormula)
                        pfrow.ordinarystructuralformula = formula
                        pfrow.stoichiometricformula = (
                            species['Molecules'][species_id]
                            .StoichiometricFormula.strip())
                        pfrow.chemicalname = (
                            species['Molecules'][species_id]
                            .ChemicalName.strip())
                        pfrow.name = formula
                        pfrow.comment = (species['Molecules'][species_id]
                                         .Comment)
                        pfrow.vamdc_species_id = (species['Molecules']
                                                  [species_id].VAMDCSpeciesID)

                        # There might be additional partition functions for
                        # nuclear spin isomers (ortho/para)
                        try:
                            pfs_nsi_dict = (species['Molecules'][species_id]
                                            .PartitionFunction)
                        except Exception:
                            pfs_nsi_dict = {}

                        if len(pfs_nsi_dict) == 0:
                            # print("   Partition function is missing: "
                            #      "Species will not be imported!")
                            species_import_failed.append(
                                (species_id, "Partition functions not found"))
                            continue

                        for pfs in pfs_nsi_dict:
                            try:
                                if 'NuclearSpinIsomer' not in pfs.__dict__:
                                    pfrow.nsi = ''
                                else:
                                    pfrow.nsi = pfs.NuclearSpinIsomer

                                # at this point there is no information yet
                                # about states and hyperfine structure.
                                # entries have to be duplicated later.
                                pfrow.name = make_name(formula, nsi=pfrow.nsi)

                                # insert new row into db (status='new')
                                #
                                # TO BE DONE: NAME FOR ORTHO etc.
                                id = self.db_insert_partitionfunction(
                                        pfrow, commit=False)

                                # update its partition function
                                for temperature in pfs.values.keys():
                                    self.update_partitionfunction_by_id(
                                        id,
                                        temperature,
                                        float(pfs.values[temperature]),
                                        commit=False
                                    )
                                self.conn.commit()
                                count_species_imported += 1
                                print("Imported new entry %s (ID: %s)!"
                                      % (pfrow.name, species_id))
                            except Exception as e:
                                self.conn.rollback()
                                if (species_id, str(e))\
                                        not in species_import_failed:
                                    species_import_failed.append((species_id,
                                                                  str(e)))

                                print("Partition functions could not be parsed"
                                      "for specie %s (nsi=%s):\n%s "
                                      % (species_id, pfrow.nsi, e))

            except Exception as e:
                error = str(e)
                species_import_failed.append((species_id, error))
                print("Species (id=%s) was not inserted due to following "
                      "error:\n %s" % (species_id, str(e)))

        print("%d new species have been imported into the database.\n\n"
              "Run update_species_data() to import their transitions!\n\n"
              % count_species_imported)
        print("Import of the following species failed:")
        for i in species_import_failed:
            print("\n%s:\n%s" % (i[0], i[1]))

    def show_species(self, status=None):
        """
        Lists all species, which are stored in the local sqlite3 database.
        """
        cursor = self.conn.cursor()
        if status is not None:
            cursor.execute("SELECT PF_ID, PF_Name, PF_SpeciesID, "
                           "PF_VamdcSpeciesID, PF_Recommendation, "
                           "PF_Status, PF_Createdate, "
                           "PF_Checkdate FROM Partitionfunctions "
                           "WHERE PF_Status = ?", (status,))
        else:
            cursor.execute("SELECT PF_ID, PF_Name, PF_SpeciesID, PF_VamdcSpeciesID, \
                            PF_Recommendation, PF_Status, PF_Createdate, \
                            PF_Checkdate FROM Partitionfunctions")
        rows = cursor.fetchall()
        for row in rows:
            print("%-5s %-10s %-60s %20s %10s %10s %s %s"
                  % (row[0], row[2], row[1], row[3], row[4],
                     row[5], row[6], row[7]))

    def delete_species(self, speciesid):
        """
        Deletes data related to a species defined by its species-id
        (All entries, e.g. all states, hfs, ...

        :ivar str speciesid: Id of the Specie
        """
        deleted_species = []
        cursor = self.conn.cursor()
        cursor.execute("SELECT PF_ID FROM Partitionfunctions WHERE "
                       "PF_SpeciesID = ?", (speciesid, ))
        rows = cursor.fetchall()
        for row in rows:
            deleted_species.append(row[0])
            cursor.execute("DELETE FROM Transitions WHERE T_PF_ID = ?",
                           (row[0], ))
            cursor.execute("DELETE FROM Partitionfunctions WHERE PF_ID = ?",
                           (row[0], ))

        self.conn.commit()
        cursor.close()

        return deleted_species

    def delete_species_entry(self, id):
        """
        Deletes data for an entry in the Partitionfunctions-Table and related
        data in Transitions.

        :ivar int id: Id of the Specie-Entry (PF_ID)
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT PF_ID FROM Partitionfunctions WHERE "
                       "PF_ID = ?", (id, ))
        rows = cursor.fetchall()
        for row in rows:
            cursor.execute("DELETE FROM Transitions WHERE T_PF_ID = ?",
                           (row[0], ))
            cursor.execute("DELETE FROM Partitionfunctions WHERE PF_ID = ?",
                           (row[0], ))

        self.conn.commit()
        cursor.close()

    def update_species_data(self, species=None, node=None, force_update=False):
        """
        Update the data for species marked as 'New' or 'Update Available'.

        :ivar list species: species which will be inserted
        :ivar nodes.Node node: vamdc-node / type: instance(nodes.node)
        :ivar boolean update:  if True then all entries in the local database
                                with the same species-id will be deleted before
                                the insert is performed.
        """
        # list will contain species whose insert failed.
        species_with_error = []

        # ----------------------------------------------------------
        # Create a list of species for which transitions will be
        # retrieved and inserted in the database.
        # Species have to be in the Partitionfunctions - table

        nl = nodes.Nodelist()

        if species is None:
            where_species = ''
        else:
            # make species iterable
            if not functions.isiterable(species):
                species = [species]
            if isinstance(species[0], str) and len(species[0]) == 27:
                # vamdcspecies-id is used
                where_species = "WHERE PF_VamdcSpeciesID in ('"\
                                + "','".join(species) + "') "
            else:
                # assume that species-id is used
                where_species = "WHERE PF_SpeciesID in ('" \
                                + "','".join(species) + "') "
        if node is not None:
            if type(node) == str:
                node = nl.findnode(node)

            if type(node) == nodes.Node:
                resource_id = node.identifier

            if len(where_species) > 0:
                where_nodes = "AND PF_ResourceID = '%s' " % resource_id
            else:
                where_nodes = "WHERE PF_ResourceID = '%s' " % resource_id
        else:
            where_nodes = ""

        # find all species that need to be updated
        cursor = self.conn.cursor()
        cursor.execute("SELECT PF_ID, PF_Name, PF_SpeciesID, "
                       "PF_VamdcSpeciesID, PF_NuclearSpinIsomer, "
                       "PF_ResourceID, PF_VibState, PF_HFS, PF_Status, "
                       "PF_ElecState "
                       "FROM Partitionfunctions "
                       "%s %s ORDER BY PF_VamdcSpeciesID, PF_SpeciesID"
                       % (where_species, where_nodes))

        # lookup table for specie_ids and corresponding nodes
        species_dict = {}
        # Lookup-table for pf_id's
        species_dict_id = {}
        for row in cursor.fetchall():
            db_id = row[0]
            # db_name = row[1]
            db_species_id = row[2]
            if row[4] is None or len(row[4]) == 0:
                db_nsi = None
            else:
                db_nsi = row[4]
            if row[6] is None or len(row[6]) == 0:
                db_vibstate = None
            else:
                db_vibstate = row[6]
            if row[7] is None or len(row[7]) == 0:
                db_hfs = None
            else:
                db_hfs = row[7]
            if row[9] is None or len(row[9]) == 0:
                db_elecstate = None
            else:
                db_elecstate = row[9]
            sidx = (db_species_id, db_nsi, db_vibstate, db_hfs, db_elecstate)
            db_vamdcspecies_id = row[3]

            # get node instance
            try:
                node = nl.findnode(row[5])
            except Exception:
                print("Node %s not found! "
                      "Check identifier in the registry!"
                      % node)
                continue

            db_status = row[8]
            if force_update or db_status.lower() in (
                    'new', 'update available', 'update failed',
                    'updating'):
                if sidx in species_dict_id:
                    print("Doublicate entry found for specie %s: "
                          "Deleting ID: %d"
                          % (db_species_id, db_id))
                    self.delete_species_entry(db_id)
                else:
                    species_dict_id[sidx] = db_id

                if db_species_id in species_dict \
                        and (node, db_vamdcspecies_id) != \
                        species_dict[db_species_id]:
                    print("Warning: Additional entry found for specie "
                          "%s " % db_species_id)

                else:
                    species_dict[db_species_id] = (node, db_vamdcspecies_id)
            else:
                # -1 indicates that this entry shall not be updated.
                species_dict_id[sidx] = -1

        cursor.close()
        # process species-id first and then vamdc-species-ids
        while species_dict:
            # dictionary thatstores processed transitions.
            transitions_processed = {}

            # get next species
            species_id = next(iter(species_dict))
            (db_node, db_vamdcspecies_id) = species_dict.pop(species_id)

            # try to retrieve data for species-id. If it fails because
            # species-id is not a standard vamdctap-keyword then try to
            # query data for vamdcspecies-id (inchikey)
            try:
                result = r.do_species_data_request(
                        db_node,
                        species_id=species_id,
                        vamdcspecies_id=db_vamdcspecies_id
                        )
            except Exception as e:
                print(" -- Error %s: Could not fetch and process data"
                      % e.strerror)
                continue
            if result is None:
                print(" -- Error %s: Could not fetch and process data")
                continue

            if species_id in result.data['Molecules']:
                species_data = result.data['Molecules']
                is_molecule = True
            elif species_id in result.data['Atoms']:
                species_data = result.data['Atoms']
                is_molecule = False
            else:
                print("Species %s not found in result! "
                      "Entry outdated! " % species_id)
                continue

            # mark rows for this specie
            # if query used vamdcspecies-id then more than one species-id
            # might have been returned, but all are either atoms or molecules.
            cursor = self.conn.cursor()
            print("Start Update")
            for sid in species_data:
                print(sid)
                # remove species from dictionary of species to process
                try:
                    (db_node, db_vamdcspecies_id) = species_dict.pop(sid)
                except KeyError:
                    # the species was removed above, but there might be
                    # different species if restrictable vamdcspecies-id
                    # was used.
                    pass
                if force_update:
                    cursor.execute("UPDATE Partitionfunctions "
                                   "SET PF_Status='Updating' "
                                   "WHERE PF_SpeciesID = ? ",
                                   (sid, ))
                else:
                    cursor.execute("UPDATE Partitionfunctions "
                                   "SET PF_Status='Updating' "
                                   "WHERE PF_SpeciesID = ? "
                                   "AND PF_Status in ('New', "
                                   "'Update Available', 'Update Failed')",
                                   (sid, ))
                # Delete all Transitions for these entries
                cursor.execute("DELETE FROM Transitions "
                               "WHERE T_PF_ID in "
                               "(SELECT PF_ID FROM Partitionfunctions "
                               " WHERE PF_SpeciesID = ? "
                               " AND PF_Status = 'Updating')",
                               (sid, ))
            self.conn.commit()

            # ------------------------------------------
            # Insert all transitions
            transitions = result.data['RadiativeTransitions']
            num_transitions_found = len(transitions)
            counter_transitions = 0
            for trans in transitions:
                transition = transitions[trans]
                counter_transitions += 1
                print(" insert transition %d of %d\r"
                      % (counter_transitions, num_transitions_found), end="")

                # data might contain transitions for other species (if query is
                # based on ichikey/vamdcspeciesid). Insert transitions only if
                # they belong to the correct specie
                #
                t_species_id = transition.SpeciesID

                # if an error has occured already then there will be no
                # further insert
                if t_species_id in species_with_error:
                    continue

                # Get upper and lower state from the states table
                try:
                    upper_state = result.get_state(
                            transition.UpperStateRef)
                    lower_state = result.get_state(
                            transition.LowerStateRef)
                except (KeyError, AttributeError):
                    print(" -- Error: State is missing")
                    species_with_error.append(t_species_id)
                    continue

                if is_molecule:
                    # Get string which identifies the vibrational states
                    # involved in the transition
                    t_state = self.getvibstatelabel(upper_state,
                                                    lower_state)
                    t_elecstate = self.getelecstatelabel(upper_state,
                                                         lower_state)
                    if len(t_state) == 0:
                        t_state = None
                    if len(t_elecstate) == 0:
                        t_elecstate = None
                    t_name = species_data[species_id].OrdinaryStructuralFormula
                else:
                    t_name = self.createatomname(species_data[species_id])
                    t_state = None
                    t_elecstate = None

                t_name = t_name.strip()

                # Get hyperfinestructure info if hfsInfo is None
                # only then the hfsInfo has not been inserted in the
                # species name (there can be multiple values in the
                # complete dataset
                t_hfs = None
                try:
                    for pc in transition.ProcessClass:
                        if str(pc)[:3] == 'hyp':
                            t_hfs = str(pc).strip()
                except Exception as e:
                    print("Error: %s", e)

                frequency = float(transition.FrequencyValue)
                try:
                    uncertainty = "%lf" % float(transition.FrequencyAccuracy)
                except TypeError:
                    print(" -- Error uncertainty not available")
                    species_with_error.append(t_species_id)
                    continue

                # Get statistical weight if present
                try:
                    weight = int(upper_state.TotalStatisticalWeight)
                except Exception:
                    print(" -- Error statistical weight not available")
                    species_with_error.append(t_species_id)
                    continue

                # Get nuclear spin isomer (ortho/para) if present
                try:
                    nsi_name = upper_state.NuclearSpinIsomerName.strip()
                except AttributeError:
                    nsi_name = None

                # if nuclear spin isomer is defined then two entries have
                # to be generated
                if nsi_name is not None and nsi_name != '':
                    try:
                        nsinames = [nsi_name, None]
                        nsi_state_origin = result.data['States'][
                            "%s" % upper_state.NuclearSpinIsomerLowestEnergy]
                        nsi_energy_offset = float(
                                nsi_state_origin.StateEnergyValue)
                    except KeyError as e:
                        print("Exception occured while parsing nuclear "
                              "spin isomers: %s" % str(e))
                        species_with_error.append(t_species_id)
                        continue
                else:
                    nsinames = [None]

                for nsiName in nsinames:
                    sidx = (t_species_id, nsiName, t_state, t_hfs, t_elecstate)

                    # update list of distinct species names.
                    if sidx not in transitions_processed:
                        transitions_processed[sidx] = 0

                    # Calculate Energy (Important for nuclear spin isomers
                    # as the offset in energy has to be taken into account.
                    if nsiName is not None:
                        lower_state_energy = \
                                float(lower_state.StateEnergyValue) \
                                - nsi_energy_offset
                    else:
                        lower_state_energy = \
                                float(lower_state.StateEnergyValue)

                    # -------------------------------------------------------------------------------------------------
                    # GET ID of the PF_Entry:
                    # Insert Specie in Partitionfunction if it does not
                    # exist there, otherwise get id only

                    db_id = species_dict_id.get(sidx)

                    # maybe states and hfs is still None
                    # (new species) -> Update these informations
                    if db_id is None:
                        try:
                            # remove in dictionary
                            db_id = species_dict_id.pop(
                                (sidx[0], sidx[1], None, None, None))
                            if db_id > 0:
                                # add again with new state value
                                species_dict_id[sidx] = db_id
                                # set state and hfs also in the database
                                self.update_pf_state(
                                        db_id, sidx[2], sidx[3], sidx[4])
                                # continue with whatever id was stored
                            else:
                                # do nothing and add old value again
                                species_dict_id[(sidx[0], sidx[1],
                                                 None, None, None)] = db_id
                        except KeyError:
                            pass

                    # Maybe the electronic State info was updated:
                    # if all other values agree and the existing entry has
                    # no transitions to import, then it is most likely that
                    # only the electronic state was updated. In case that
                    # these transitions will be processed later, the old
                    # entry will get a new database-id, which should not matter
                    # at all (Usually there is only one electronic state per
                    # species-id (CDMS/JPL). The same could happen for the
                    # vibrational state or both states at the same time
                    if db_id is None:
                        sidx_match_without_elec = None
                        sidx_match_without_vib = None
                        sidx_match_without_both = None
                        for sidx_stored in species_dict_id:
                            sidx_match = tuple(i == j
                                               for i, j in
                                               zip(sidx_stored, sidx))

                            if (sidx_match == (True, True, True, True, False)
                                    and sidx_stored
                                    not in transitions_processed):
                                sidx_match_without_elec = sidx_stored

                            if (sidx_match == (True, True, False, True, True)
                                    and sidx_stored
                                    not in transitions_processed):
                                sidx_match_without_vib = sidx_stored

                            if (sidx_match == (True, True, False, True, False)
                                    and sidx_stored
                                    not in transitions_processed):
                                sidx_match_without_elec = sidx_stored

                        # assign new new sidx to the old entry
                        # priority : First elec, then vib, last both
                        if sidx_match_without_elec is not None:
                            db_id = species_dict_id.pop(
                                    sidx_match_without_elec)
                        elif sidx_match_without_vib is not None:
                            db_id = species_dict_id.pop(
                                    sidx_match_without_vib)
                        elif sidx_match_without_both is not None:
                            db_id = species_dict_id.pop(
                                    sidx_match_without_both)

                    if db_id is not None:
                        # assign new sidx
                        species_dict_id[sidx] = db_id
                        # set states also in the database
                        self.update_pf_state(
                                db_id, sidx[2], sidx[3], sidx[4])
                    else:
                        # New Row has to be created
                        try:
                            # entry does not exist yet, so creeate it.
                            db_id = self.doublicate_pf_entry_for_hfs(
                                        species_id,
                                        sidx[0],
                                        sidx[1],
                                        sidx[2],
                                        sidx[3],
                                        sidx[4])

                            species_dict_id[sidx] = db_id
                        except sqlite3.Error as e:
                            print("An error occurred:", str(e))
                        except Exception as e:
                            print("An error occurred:", str(e))
                            print(result.data['Molecules'].keys())

                    # negative id's indicate that transition must not be
                    # inserted
                    if db_id is not None and db_id < 0:
                        continue

                    # ------------------------------------------------------------------------------
                    # Insert transition into database
                    try:
                        cursor.execute(
                            """INSERT INTO Transitions (
                                T_PF_ID,
                                T_Name,
                                T_Frequency,
                                T_EinsteinA,
                                T_Uncertainty,
                                T_EnergyLower,
                                T_UpperStateDegeneracy,
                                T_HFS,
                                T_UpperStateQuantumNumbers,
                                T_LowerStateQuantumNumbers) VALUES
                                (?,?,?,?,?,?,?,?,?,?)""",
                            (db_id,
                             make_name(t_name, nsiName, t_state, t_hfs),
                             "%lf" % frequency,
                             "%g" % float(transition.TransitionProbabilityA),
                             uncertainty,
                             "%lf" % lower_state_energy,
                             weight,
                             t_hfs,
                             str(upper_state.QuantumNumbers.qn_string),
                             str(lower_state.QuantumNumbers.qn_string),
                             ))
                        transitions_processed[sidx] += 1
                    except Exception as e:
                        print("Transition has not been inserted:\n Error: %s"
                              % e)
                        species_with_error.append(t_species_id)
                        continue
            print("\n")
            # ------------------------------------------------------------------------------------------------------

            # ------------------------------------------------------------------------------------------------------
            # delete transitions for all entries where an error occured during
            # the insert
            if species_id in species_with_error:
                print(" -- Species {id} has not been updated due to an error "
                      .format(id=str(species_id)))
                try:
                    cursor.execute("DELETE FROM Transitions WHERE T_PF_ID in "
                                   "(SELECT PF_ID FROM Partitionfunctions "
                                   " WHERE PF_SpeciesID=? "
                                   " AND PF_Status='Updating')",
                                   (str(species_id),))
                except Exception as e:
                    print(" -> Tried to remove transitions for that species, "
                          "but an exception occured:\n %s" % str(e))

                self.set_status(species_id, 'Update Failed')

            # ------------------------------------------------------------------------------------------------------
            # insert specie in Partitionfunctions (header) table

            # -------------------------------------------------------------------
            # Update Partitionfunctions
            if species_id in species_data:
                # partition fucntions for atoms have to be calculated based
                # on the state energies.
                self.parse_and_update_partitionfunctions(species_id, result)

            # ------------------------------------------------------------------------------------------------------
            for sidx in transitions_processed:
                if sidx[0] in species_with_error:
                    continue

                print("      species %s %s %s %s: imported %d transitions"
                      % (sidx[0], sidx[1], sidx[2], sidx[3],
                         transitions_processed[sidx]))

                # insert the query - identifier into db
                db_id = species_dict_id[sidx]
                self.set_uuid(db_id, result.get_uuid())

                self.set_status(sidx[0],
                                'Up-To-Date',
                                species_dict_id.get(sidx))
            # remove entries with status 'updating', but where not processed.
            # This should not happen, but in case there are doublicates with
            # different elecstates for example i could happen.
            cursor.execute("SELECT PF_ID FROM Partitionfunctions "
                           "WHERE PF_SpeciesID = ? "
                           "AND PF_Status = 'Updating'", (species_id,))
            rows_to_delete = cursor.fetchall()
            for row_td in rows_to_delete:
                print("-- Clean database: Entry id=%d was untouched "
                      "and is not in a clean state. It will be deleted."
                      % row_td[0])
                self.delete_species_entry(row_td[0])

            self.conn.commit()
            cursor.close()
        # delete transitions for all entries where an error occured during
        # the insert
        for id in species_with_error:
            print(" -- Species {id} has not been updated due to an error "
                  .format(id=str(id)))

    def update_database(
            self,
            add_nodes=None,
            insert_only=False,
            update_only=False,
            delete_archived=False):
        """
        Checks if there are updates available for all entries. Updates will be
        retrieved from the resource specified in the database.  All resources
        will be searched for new entries, which will be inserted if available.
        Additional resources can be specified via add_nodes.

        :ivar nodes.Node add_nodes: Single or List of node-instances.
        :ivar boolean insert_only: Insert new species and skip updates
        :ivar boolean update_only: Updates species and skip inserts
        """
        # Check if updates are available in VAMDC-Databases
        if not insert_only:
            print("--------------------------------------")
            print("CHECK DATABASES FOR UPDATES")
            self.check_for_updates()

        if not update_only:

            # create an instance with all available vamdc-nodes
            nl = nodes.Nodelist()

            # list of database - nodes which are currently in the local
            # database
            dbnodes = []
            # get all resources / databases that are used so far
            cursor = self.conn.cursor()
            cursor.execute("SELECT DISTINCT PF_ResourceID "
                           "FROM Partitionfunctions")
            rows = cursor.fetchall()
            for row in rows:
                dbnodes.append(nl.getnode(row[0]))

            # attach additional nodes to the list of dbnodes (for insert)
            if not functions.isiterable(add_nodes):
                add_nodes = [add_nodes]

            # add additional nodes
            for node in add_nodes:
                if node is None:
                    pass
                elif not isinstance(node, nodes.Node):
                    print("Could not attach node. Wrong type, "
                          "it should be type <nodes.Node>")
                else:
                    dbnodes.append(node)

            for node in dbnodes:
                print("--------------------------------------")
                print("CHECK DATABASE %s FOR NEW SPECIES" % node.name)
                self.check_for_new_species(node)

        self.update_species_data()

        self.update_all_recommendations()

    def parse_and_update_partitionfunctions(self, id, result):
        """
        Parse the result and update partitionfunctions for specie with given
        id.
        """
        calc_part = True

        # XSAMS - Atom section does not have a section with partitionfunctions.
        # Thus only partitionfunctions for molecules can be found in the
        # result.  and others need to be calculated based on state energies.
        try:
            if id in result.data['Molecules'].keys():
                pfs_nsi_dict = result.data['Molecules'][id].PartitionFunction
                calc_part = False
        except Exception:
            pass

        if calc_part:
            # calculate partitionfunctions from state energies
            for temperature in TEMPERATURES:
                try:
                    pf_values = specmodel.calculate_partitionfunction(
                        result.data['States'],
                        temperature=temperature)
                except Exception as e:
                    print("Calculation of partition functions failed for "
                          "specie %s:\n%s " % (id, str(e)))

                self.update_partitionfunction(id, temperature, pf_values[id])

        else:
            for pfs in pfs_nsi_dict:
                try:
                    if 'NuclearSpinIsomer' not in pfs.__dict__:
                        nsi = ''
                    else:
                        nsi = pfs.NuclearSpinIsomer

                    for temperature in pfs.values.keys():
                        self.update_partitionfunction(
                                id,
                                temperature,
                                pfs.values[temperature],
                                nsi)
                except Exception as e:
                    print("Partition functions could not be parsed for "
                          "specie %d (nsi=%s):\n%s " % (id, nsi, e))

    def update_partitionfunction(self, id, temperature, value, nsi=''):
        """
        Update the partition function in the database for an atom/molecule for
        the given temperature

        :var id: species-id
        :type id:int
        :var temperature: temperature (the corresponding field has to
                          exist in the database
        :type temperature: float
        :par value: value of the partitionfunction
        :type value: float
        :par nsi: nuclear spin identifier
        :type nsi. str
        """
        cursor = self.conn.cursor()
        try:
            field = ("PF_%.3lf" % float(temperature)).replace('.', '_')
            sql = ("UPDATE Partitionfunctions "
                   "SET %s=? WHERE PF_SpeciesID=? "
                   "AND IFNULL(PF_NuclearSpinIsomer,'')=?" % field)

            cursor.execute(sql, (value, id, nsi))
        except Exception as e:
            print("SQL-Error occred while updating partitionfunction for "
                  "species-id %d and temperature %lf\nsql: %s "
                  % (id, temperature, sql))
            print("Error: %s" % e)
        cursor.close()

    def update_partitionfunction_by_id(self,
                                       id,
                                       temperature,
                                       value,
                                       commit=True):
        """
        Update the partition function in the database for a given entry in the
        database.

        :var id: sqlite3 db id
        :type id:int
        :var temperature: temperature (the corresponding field has to
                          exist in the database
        :type temperature: float
        :par value: value of the partitionfunction
        :type value: float
        :par commit: Commit changes in database
        :type commit: boolean
        """
        cursor = self.conn.cursor()
        try:
            field = ("PF_%.3lf" % float(temperature)).replace('.', '_')
            sql = ("UPDATE Partitionfunctions "
                   "SET %s=? WHERE PF_ID=? " % field)

            cursor.execute(sql, (value, id))
            if commit:
                self.conn.commit()
        except Exception as e:
            print("SQL-Error occred while updating partitionfunction for "
                  "species-id %d and temperature %lf\nsql: %s "
                  % (id, temperature, sql))
            print("Error: %s" % e)
        cursor.close()

    # ********************************************************************
    def getvibstatelabel(self, upper_state, lower_state):
        """
        Creates vibrational state label for a transition.

        :ivar specmodel.State upper_state: state instance of the upper state
        :ivar specmodel.State lower_state: state instance of the lower state
        :return: vibrational state label for the transition
        :rtype: str
        """

        # Get string which identifies the vibrational states involved in the
        # transition
        if (upper_state.QuantumNumbers.vibstate ==
                lower_state.QuantumNumbers.vibstate):
            t_state = str(upper_state.QuantumNumbers.vibstate).strip()
        else:
            v_dict = {}
            for label in set(upper_state.QuantumNumbers.qn_dict) \
                    | set(lower_state.QuantumNumbers.qn_dict):
                if specmodel.isVibrationalStateLabel(label):
                    try:
                        value_up = upper_state.QuantumNumbers.qn_dict[label]
                    except Exception:
                        value_up = 0
                    try:
                        value_low = lower_state.QuantumNumbers.qn_dict[label]
                    except Exception:
                        value_low = 0
                    v_dict[label] = [value_up, value_low]
            v_string = ''
            valup_string = ''
            vallow_string = ''
            for v in v_dict:
                v_string += "%s," % v
                valup_string += "%s," % v_dict[v][0]
                vallow_string += "%s," % v_dict[v][1]
            # do not distinct between upper and lower state
            # create just one label for both cases
            if valup_string < vallow_string:
                dummy = vallow_string
                vallow_string = valup_string
                valup_string = dummy
            if len(v_dict) > 1:
                t_state = "(%s)=(%s)-(%s)" % (
                        v_string[:-1],
                        valup_string[:-1],
                        vallow_string[:-1])
            else:
                t_state = "%s=%s-%s" % (
                        v_string[:-1],
                        valup_string[:-1],
                        vallow_string[:-1])

        return t_state.strip()

    def getelecstatelabel(self, upper_state, lower_state):
        if (upper_state.QuantumNumbers.elecstate ==
                lower_state.QuantumNumbers.elecstate):
            t_state = str(upper_state.QuantumNumbers.elecstate).strip()
        else:
            t_state = "%s-%s" % (
                    upper_state.QuantumNumbers.elecstate.strip(),
                    lower_state.QuantumNumbers.elecstate.strip())
        return t_state.strip()

    # *******************************************************************
    def createatomname(self, atom):
        """
        Creates a name for an atom. The format is
        (massnumber)(elementsymbol)(charge): e.g. 13C+, 12C+

        :ivar specmodel.Atom atom: Atom for which the name will be created
        :return: Name for the atom
        :rtype: str
        """

        try:
            charge = int(atom.IonCharge)
        except AttributeError:
            charge = 0

        if charge == 0:
            charge_str = ''
        elif charge == 1:
            charge_str = '+'
        elif charge == -1:
            charge_str = '-'
        else:
            charge_str = str(charge)

        try:
            massnumber = atom.MassNumber
        except AttributeError:
            massnumber = ''

        name = "%s%s%s" % (massnumber, atom.ChemicalElementSymbol, charge_str)
        return name.strip()

    def get_reference_url(self, id):
        """
        returns an url that show the original xsams-result and the references.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT PF_UUID FROM Partitionfunctions "
                       "WHERE PF_ID = ?", (id,))
        row = cursor.fetchone()
        try:
            uuid = row[0]
            url = 'https://cite.vamdc.eu/references.html?uuid=%s' % uuid
        except Exception:
            url = None
        return url

    def update_recommendation(self, id):
        """
        Updates the recommendation for an entry.
        """
        is_recommended = r.get_recommendation(id)
        if is_recommended is None:
            return

        cursor = self.conn.cursor()
        cursor.execute("UPDATE Partitionfunctions "
                       "SET PF_Recommendation = ? "
                       "WHERE PF_SpeciesID = ? ",
                       (is_recommended, id))
        self.conn.commit()
        cursor.close()

    def update_all_recommendations(self):
        """
        Loops through all the entries and checks if they are recommended
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT PF_SpeciesID "
                       "FROM Partitionfunctions ")

        rows = cursor.fetchall()
        for row in rows:
            self.update_recommendation(row[0])
