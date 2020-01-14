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


class PFrow(object):
    """
    """
    def __init__(self, **kwds):
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
                                    hfs):
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
                       (hfs, state, species_id, nsi))

        self.conn.commit()
        db_id = cursor.lastrowid
        cursor.close()
        return db_id

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

    def update_pf_state(self, id, state, hfs, commit=True):
        """
        Updates state and hyperfine structure information
        for new entries.
        """
        cursor = self.conn.cursor()
        cursor.execute("UPDATE PartitionFunctions "
                       "SET PF_VibState = ?, "
                       "    PF_HFS = ? "
                       "WHERE PF_ID = ? ",
                       (state, hfs, id))
        if commit:
            self.conn.commit()
        cursor.close()

    def check_for_updates(self, node):
        """
        Checks for each database entry if an update for the molecular or atomic
        specie is available in the specified VAMDC database node.
        Only head-requests are performed and only the last-modified date will
        be retrieved. Updates have to be called separately.

        :ivar nodes.Node node: VAMDC database node which will be checked
                               for updates
        """
        count_updates = 0
        counter = 0
        cursor = self.conn.cursor()
        cursor.execute("SELECT PF_Name, PF_SpeciesID, PF_VamdcSpeciesID, "
                       "datetime(PF_Checkdate), PF_Status "
                       "FROM Partitionfunctions ")
        rows = cursor.fetchall()
        num_rows = len(rows)
        request = r.Request()

        for row in rows:
            counter += 1
            print("%5d/%5d: Check specie %-55s (%-15s): "
                  % (counter, num_rows, row[0], row[1]), end=' ')
            species_id = row[1]
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
                            print("   Partition function is missing: "
                                  "Species will not be imported!")
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
        Deletes species stored in the database

        :ivar str speciesid: Id of the Specie
        """
        deleted_species = []
        cursor = self.conn.cursor()
        cursor.execute("SELECT PF_Name FROM Partitionfunctions WHERE "
                       "PF_SpeciesID = ?", (speciesid, ))
        rows = cursor.fetchall()
        for row in rows:
            deleted_species.append(row[0])
            cursor.execute("DELETE FROM Transitions WHERE T_Name = ?",
                           (row[0], ))
            cursor.execute("DELETE FROM Partitionfunctions WHERE PF_Name = ?",
                           (row[0], ))

        self.conn.commit()
        cursor.close()

        return deleted_species

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
                       "PF_ResourceID, PF_VibState, PF_HFS, PF_Status "
                       "FROM Partitionfunctions "
                       "%s %s ORDER BY PF_VamdcSpeciesID, PF_SpeciesID"
                       % (where_species, where_nodes))

        species_dict = {}
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
            sidx = (db_species_id, db_nsi, db_vibstate, db_hfs)
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
                # remove species from dictionary of species to process
                try:
                    (db_node, db_vamdcspecies_id) = species_dict.pop(sid)
                except KeyError:
                    # the species was removed above, but there might be
                    # different species if restrictable vamdcspecies-id
                    # was used.
                    pass
                cursor.execute("UPDATE Partitionfunctions "
                               "SET PF_Status='Updating' "
                               "WHERE PF_SpeciesID = ? "
                               "AND PF_Status in ('New', 'Update Available', "
                               "'Update Failed')",
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
                    if len(t_state) == 0:
                        t_state = None
                    t_name = species_data[species_id].OrdinaryStructuralFormula
                else:
                    t_name = self.createatomname(species_data[species_id])
                    t_state = None
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
                    sidx = (t_species_id, nsiName, t_state, t_hfs)

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

                    # maybe the state is None
                    if db_id is None:
                        try:
                            # remove in dictionary
                            db_id = species_dict_id.pop(
                                (sidx[0], sidx[1], None, None))
                            if db_id > 0:
                                # add again with new state value
                                species_dict_id[sidx] = db_id
                                # set state and hfs also in the database
                                self.update_pf_state(db_id, sidx[2], sidx[3])
                                # continue with whatever id was stored
                            else:
                                # do nothing and add old value again
                                species_dict_id[
                                    (sidx[0], sidx[1], None, None)] = db_id
                        except KeyError:
                            pass

                    if db_id is None:
                        try:
                            # entry does not exist yet, so creeate it.
                            db_id = self.doublicate_pf_entry_for_hfs(
                                        species_id,
                                        sidx[0],
                                        sidx[1],
                                        sidx[2],
                                        sidx[3])

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
            print("\n")
            # ------------------------------------------------------------------------------------------------------

            # ------------------------------------------------------------------------------------------------------
            # delete transitions for all entries where an error occured during
            # the insert
            for id in species_with_error:
                print(" -- Species {id} has not been updated due to an error "
                      .format(id=str(id)))
                try:
                    cursor.execute("DELETE FROM Transitions WHERE T_PF_ID in "
                                   "(SELECT PF_ID FROM Partitionfunctions "
                                   " WHERE PF_SpeciesID=? "
                                   " AND PF_Status='Updating')", (str(id),))
                except Exception as e:
                    print(" -> Tried to remove transitions for that species, "
                          "but an exception occured:\n %s" % str(e))

                self.set_status(id, 'Update Failed')

            # ------------------------------------------------------------------------------------------------------
            # insert specie in Partitionfunctions (header) table

            # -------------------------------------------------------------------
            # Update Partitionfunctions
            if not is_molecule and species_id in species_data:
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
            self.conn.commit()
            cursor.close()

    def update_db_species(self, species, node):
        """
        Checks the VAMDC database node for new species and inserts them into
        the local database

        :ivar list species: species which will be inserted
        :ivar nodes.Node node: vamdc-node / type: instance(nodes.node)
        :ivar boolean update:  if True then all entries in the local database
                                with the same species-id will be deleted before
                                the insert is performed.
        """
        if node:
            resource_id = node.identifier
            url = node.url
        else:
            resource_id = 'NULL'
            url = 'NULL'

        # ----------------------------------------------------------
        # Create a list of species for which transitions will be
        # retrieved and inserted in the database.
        # Species have to be in the Partitionfunctions - table

        if not functions.isiterable(species):
            species = [species]

        for specie in species:
            # if species is a dictionary (e.g. specmodel.Molecules)
            # then get the species-instance instead of only the key.
            if isinstance(species, dict):
                specie = species[specie]

            num_transitions = {}
            # will contain a list of names which belong to one specie
            species_names = {}
            # list will contain species whose insert-failed
            species_with_error = []

            # check if specie is of type Molecule
            if isinstance(specie, specmodel.Molecule):
                speciesid = specie.SpeciesID
                vamdcspeciesid = specie.VAMDCSpeciesID
                formula = specie.OrdinaryStructuralFormula
            if isinstance(specie, specmodel.Atom):
                speciesid = specie.SpeciesID
                vamdcspeciesid = specie.VAMDCSpeciesID

            # check if the specie is identified by its inchikey
            elif isinstance(specie, str) and len(specie) == 27:
                vamdcspeciesid = specie
                speciesid = None
            else:
                vamdcspeciesid = None
                speciesid = specie

            if speciesid:
                print("Processing: {speciesid}".format(speciesid=speciesid))
                print("Be aware that not all VAMDC-Nodes are able to query "
                      " SpeciesID's")
                # Create query string
                query_string = "SELECT ALL WHERE SpeciesID='%s'" % speciesid
            else:
                print("Processing: {vamdcspeciesid}".format(
                    vamdcspeciesid=vamdcspeciesid))
                # Create query string
                query_string = \
                    "SELECT ALL WHERE VAMDCSpeciesID='%s'" % vamdcspeciesid

            # Query the database. The query uses the vamdcspeciesid
            # (InChI-Key), because this is mandatory for all databases
            # (SpecieID is not).
            try:
                request = r.Request()

                # Get data from the database
                request.setnode(node)
                request.setquery(query_string)

                result = request.dorequest()
            except Exception as e:
                print(" -- Error %s: Could not fetch and process data"
                      % e.strerror)
                continue

            if vamdcspeciesid is None:
                if speciesid in result.data['Molecules']:
                    vamdcspeciesid = \
                            result.data['Molecules'][speciesid].VAMDCSpeciesID
                elif speciesid in result.data['Atoms']:
                    vamdcspeciesid = \
                            result.data['Atoms'][speciesid].VAMDCSpeciesID
                else:
                    print("Could not determine VAMDCSpeciesID")

            # -----------------------------------------------------
            # Check what is in the database for this species
            # and create a look-up table for id to reduce database traffic
            cursor_specie = self.conn.cursor()
            species_dict_id = {}
            cursor_specie.execute(
                "SELECT "
                "  PF_ID, "
                "  PF_SpeciesID,"
                "  PF_VamdcSpeciesID,"
                "  PF_Name,"
                "  PF_NuclearSpinIsomer,"
                "  PF_HFS,"
                "  PF_VibState,"
                "  PF_Status "
                "FROM Partitionfunctions "
                "WHERE PF_VamdcSpeciesID='{vsi}'".format(vsi=vamdcspeciesid))
            rows_specie = cursor_specie.fetchall()
            for row_specie in rows_specie:
                species_dict_id[(row_specie[1],
                                 row_specie[4],
                                 row_specie[5],
                                 row_specie[6])] = row_specie[0]
            cursor_specie.close()

            # -----------------------------------------
            cursor = self.conn.cursor()
            if speciesid is not None:
                cursor.execute(
                    "SELECT PF_ID FROM Partitionfunctions "
                    "WHERE PF_SpeciesID='{si}'".format(si=speciesid))
            else:
                cursor.execute(
                    "SELECT PF_ID FROM Partitionfunctions "
                    "WHERE PF_VamdcSpeciesID='{si}' "
                    "  AND PF_ResourceID='{rid}'"
                    .format(
                            si=vamdcspeciesid,
                            rid=resource_id))
                print("SELECT PF_ID FROM Partitionfunctions "
                      "WHERE PF_VamdcSpeciesID='{si}' "
                      "  AND PF_ResourceID='{rid}'"
                      .format(
                          si=vamdcspeciesid,
                          rid=resource_id))

            rows = cursor.fetchall()
            for row in rows:
                print("Process %d" % row[0])
                cursor.execute("""UPDATE Partitionfunctions SET
                                    PF_Status='Processing Update'
                                    WHERE PF_ID={pid}""".format(pid=row[0]))
                cursor.execute("""DELETE FROM Transitions WHERE
                                    T_PF_ID={pid}""".format(pid=row[0]))

            cursor.close()

            # ---------------------------------------
            cursor = self.conn.cursor()
            cursor.execute('BEGIN TRANSACTION')

            # ------------------------------------------
            # Insert all transitions
            num_transitions_found = len(result.data['RadiativeTransitions'])
            counter_transitions = 0
            for trans in result.data['RadiativeTransitions']:
                counter_transitions += 1
                print("\r insert transition %d of %d"
                      % (counter_transitions, num_transitions_found))

                # data might contain transitions for other species (if query is
                # based on ichikey/vamdcspeciesid). Insert transitions only if
                # they belong to the correct specie

                if result.data['RadiativeTransitions'][trans].SpeciesID \
                        == speciesid or speciesid is None:
                    id = str(
                        result.data['RadiativeTransitions'][trans].SpeciesID)
                    # if an error has occured already then there will be no
                    # further insert
                    if id in species_with_error:
                        continue

                    # Get upper and lower state from the states table
                    try:
                        upper_state = result.data['States'][
                                "%s" % result.data[
                                    'RadiativeTransitions'][
                                        trans].UpperStateRef]
                        lower_state = result.data['States'][
                                "%s" % result.data[
                                    'RadiativeTransitions'][
                                        trans].LowerStateRef]
                    except (KeyError, AttributeError):
                        print(" -- Error: State is missing")
                        species_with_error.append(id)
                        continue

                    if id in result.data['Atoms'].keys():
                        is_atom = True
                        atomname = self.createatomname(
                                result.data['Atoms'][id]).strip()
                    elif id in result.data['Molecules'].keys():
                        is_atom = False
                        formula = str(result.data[
                            'Molecules'][id].OrdinaryStructuralFormula).strip()

                        # Get string which identifies the vibrational states
                        # involved in the transition
                        t_state = self.getvibstatelabel(upper_state,
                                                        lower_state).strip()
                    else:
                        continue

                    # Get hyperfinestructure info if hfsInfo is None
                    # only then the hfsInfo has not been inserted in the
                    # species name (there can be multiple values in the
                    # complete dataset
                    t_hfs = ''
                    try:
                        for pc in result.data[
                                'RadiativeTransitions'][trans].ProcessClass:
                            if str(pc)[:3] == 'hyp':
                                t_hfs = str(pc).strip()
                    except Exception as e:
                        print("Error: %s", e)

                    frequency = float(result.data[
                        'RadiativeTransitions'][trans].FrequencyValue)
                    try:
                        uncertainty = "%lf" % float(result.data[
                            'RadiativeTransitions'][trans].FrequencyAccuracy)
                    except TypeError:
                        print(" -- Error uncertainty not available")
                        species_with_error.append(id)
                        continue

                    # Get statistical weight if present
                    try:
                        weight = int(upper_state.TotalStatisticalWeight)
                    except Exception:
                        print(" -- Error statistical weight not available")
                        species_with_error.append(id)
                        continue

                    # Get nuclear spin isomer (ortho/para) if present
                    try:
                        nsi_name = upper_state.NuclearSpinIsomerName.strip()
                    except AttributeError:
                        nsi_name = None

                    # if nuclear spin isomer is defined then two entries have
                    # to be generated
                    if nsi_name is not None and nsi_name != '':
                        nsinames = [nsi_name, None]
                        nsi_state_origin = result.data['States'][
                            "%s" % upper_state.NuclearSpinIsomerLowestEnergy]
                        nsi_energy_offset = float(
                                nsi_state_origin.StateEnergyValue)
                    else:
                        nsinames = [None]

                    for nsiName in nsinames:
                        # create name
                        if is_atom:
                            t_name = atomname
                        else:
                            t_affix = ";".join(
                                    [affix for affix in [t_hfs, nsiName]
                                        if affix is not None and affix != ''])
                            t_name = "%s;%s;%s" % (formula, t_state, t_affix)
                        t_name = t_name.strip()
                        # remove all blanks in the name
                        t_name = t_name.replace(' ', '')

                        # update list of distinct species names.
                        if id in species_names:
                            if t_name not in species_names[id]:
                                species_names[id].append(t_name)
                                num_transitions[t_name] = 0
                        else:
                            species_names[id] = [t_name]
                            num_transitions[t_name] = 0

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

                        db_id = species_dict_id.get(
                                (id, nsiName, t_hfs, t_state))

                        if db_id is None:
                            try:
                                if is_atom:
                                    if ('Comment' not in
                                            result.data['Atoms'][id].__dict__):
                                        result.data['Atoms'][id].Comment = ""
                                    formula = "%s" % \
                                        (result.data['Atoms'][id]
                                         .ChemicalElementSymbol +
                                         result.data['Atoms'][id]
                                         .ChemicalElementNuclearCharge)

                                    cursor.execute(
                                        """INSERT INTO Partitionfunctions
                                            (PF_Name,
                                            PF_SpeciesID,
                                            PF_VamdcSpeciesID,
                                            PF_StoichiometricFormula,
                                            PF_OrdinaryStructuralFormula,
                                            PF_Comment,
                                            PF_ResourceID,
                                            PF_URL,
                                            PF_Checkdate) VALUES
                                            (?,?,?,?,?,?,?,?,?)""",
                                        ("%s" % t_name,
                                            id,
                                            "%s" % (result.data['Atoms'][id]
                                                    .VAMDCSpeciesID),
                                            formula,
                                            formula,
                                            "%s" % (result.data['Atoms'][id]
                                                    .Comment),
                                            resource_id,
                                            "%s%s%s" % (url, URL_STRING, id),
                                            datetime.now(), ))
                                else:
                                    cursor.execute(
                                        """INSERT INTO Partitionfunctions
                                            (PF_Name,
                                            PF_SpeciesID,
                                            PF_VamdcSpeciesID,
                                            PF_StoichiometricFormula,
                                            PF_OrdinaryStructuralFormula,
                                            PF_ChemicalName,
                                            PF_HFS,
                                            PF_NuclearSpinIsomer,
                                            PF_VibState,
                                            PF_Comment,
                                            PF_ResourceID,
                                            PF_URL,
                                            PF_Checkdate) VALUES
                                            (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                        ("%s" % t_name,
                                         id,
                                         "%s" % (result.data['Molecules'][id]
                                                 .VAMDCSpeciesID),
                                         "%s" % (result.data['Molecules'][id]
                                                 .StoichiometricFormula),
                                         "%s" % (result.data['Molecules'][id]
                                                 .OrdinaryStructuralFormula),
                                         "%s" % (result.data['Molecules'][id]
                                                 .ChemicalName),
                                         t_hfs,
                                         nsi_name,
                                         t_state,
                                         "%s" % (result.data['Molecules'][id]
                                                 .Comment),
                                         resource_id,
                                         "%s%s%s" % (url, URL_STRING, id),
                                         datetime.now(), ))

                                db_id = cursor.lastrowid
                                species_dict_id[(id, nsiName, t_hfs, t_state)]\
                                    = db_id
                            except sqlite3.Error as e:
                                print("An error occurred:", e.args[0])
                            except Exception as e:
                                print("An error occurred:", e.args[0])
                                print(result.data['Molecules'].keys())
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
                                (
                                    db_id,
                                    t_name,
                                    "%lf" % frequency,
                                    "%g" %
                                    float(result.data['RadiativeTransitions']
                                          [trans].TransitionProbabilityA),
                                    uncertainty, "%lf" % lower_state_energy,
                                    weight,
                                    t_hfs,
                                    str(upper_state.QuantumNumbers.qn_string),
                                    str(lower_state.QuantumNumbers.qn_string),
                                    ))
                            num_transitions[t_name] += 1
                        except Exception as e:
                            print("Transition has not been inserted:\n"
                                  "Error: %s" % e)
            print("\n")
            # ------------------------------------------------------------------------------------------------------

            # ------------------------------------------------------------------------------------------------------
            # delete transitions for all entries where an error occured during
            # the insert
            for id in species_with_error:
                print(" -- Species {id} has not been inserted due to an error "
                      .format(id=str(id)))
                try:
                    cursor.execute("""DELETE FROM Transitions WHERE
                                    T_SpeciesID=?""", (str(id),))
                except Exception:
                    pass

            # ------------------------------------------------------------------------------------------------------
            # insert specie in Partitionfunctions (header) table

            # -------------------------------------------------------------------
            # Update Partitionfunctions
            if id in result.data['Atoms'].keys():
                self.parse_and_update_partitionfunctions(id, result)

            # ------------------------------------------------------------------------------------------------------
            for row in num_transitions:
                print("      for %s inserted %d transitions"
                      % (row, num_transitions[row]))
            self.conn.commit()
            cursor.close()

    # ********************************************************************
    def insert_species_data(self, species, node, update=False):
        """
        Checks the VAMDC database node for new species and inserts them into
        the local database

        :ivar list species: species which will be inserted
        :ivar nodes.Node node: vamdc-node / type: instance(nodes.node)
        :ivar boolean update:  if True then all entries in the local database
                               with the same species-id will be deleted before
                               the insert is performed.
        """

        # create a list of names. New names have not to be in that list
        names_black_list = []

        cursor = self.conn.cursor()
        cursor.execute("SELECT PF_Name FROM Partitionfunctions")
        rows = cursor.fetchall()
        for row in rows:
            names_black_list.append(row[0])

        # ----------------------------------------------------------
        # Create a list of species for which transitions will be
        # retrieved and inserted in the database.
        # Species have to be in the Partitionfunctions - table

        if not functions.isiterable(species):
            species = [species]

        # --------------------------------------------------------------

        for specie in species:
            # if species is a dictionary (e.g. specmodel.Molecules)
            # then get the species-instance instead of only the key.
            if isinstance(species, dict):
                specie = species[specie]

            num_transitions = {}
            # will contain a list of names which belong to one specie
            species_names = {}
            # list will contain species whose insert-failed
            species_with_error = []

            # check if specie is of type Molecule
            if isinstance(specie, specmodel.Molecule):
                speciesid = specie.SpeciesID
                vamdcspeciesid = specie.VAMDCSpeciesID
                formula = specie.OrdinaryStructuralFormula
            if isinstance(specie, specmodel.Atom):
                speciesid = specie.SpeciesID
                vamdcspeciesid = specie.VAMDCSpeciesID
            else:
                # check if the specie is identified by its inchikey
                try:
                    if isinstance(specie, str) and len(specie) == 27:
                        vamdcspeciesid = specie
                        speciesid = None
                except Exception:
                    print("Specie is not of wrong type")
                    print("Type Molecule or string (Inchikey) is allowed")
                    continue
            if speciesid:
                print("Processing: {speciesid}"
                      .format(speciesid=speciesid))
            else:
                print("Processing: {vamdcspeciesid}"
                      .format(vamdcspeciesid=vamdcspeciesid))

            try:
                # Create query string
                query_string = ("SELECT ALL WHERE VAMDCSpeciesID='%s'"
                                % vamdcspeciesid)
                request = r.Request()

                # Get data from the database
                request.setnode(node)
                request.setquery(query_string)

                result = request.dorequest()
            except Exception as e:
                print(" -- Error %s: Could not fetch and process data"
                      % e.strerror)
                continue
            # ---------------------------------------

            cursor = self.conn.cursor()
            cursor.execute('BEGIN TRANSACTION')

            # ----------------------------------------------------------------
            # if update is allowed then all entries in the database for the
            # given species-id will be deleted, and thus replaced by the new
            # data
            if update:
                if speciesid is None:
                    for sid in set(result.data['Molecules'])\
                             | set(result.data['Atoms']):
                        deleted_species = self.delete_species(sid)
                        for ds in deleted_species:
                            names_black_list.remove(ds)
                else:
                    deleted_species = self.delete_species(speciesid)
                    for ds in deleted_species:
                        names_black_list.remove(ds)

            # -----------------------------------------------------------------
            # Insert all transitions
            num_transitions_found = len(result.data['RadiativeTransitions'])
            counter_transitions = 0
            for trans in result.data['RadiativeTransitions']:
                counter_transitions += 1
                print("\r insert transition %d of %d"
                      % (counter_transitions, num_transitions_found), end=' ')
                # data might contain transitions for other species (if query is
                # based on ichikey/vamdcspeciesid).  Insert transitions only if
                # they belong to the correct specie

                if (result.data['RadiativeTransitions'][trans].SpeciesID ==
                        speciesid or speciesid is None):
                    id = str(result.data['RadiativeTransitions']
                             [trans].SpeciesID)

                    # if an error has occured already then there will be no
                    # further insert
                    if id in species_with_error:
                        continue

                    # Get upper and lower state from the states table
                    try:
                        upper_state = result.data[
                                'States']["%s" % result.data[
                                    'RadiativeTransitions'][
                                        trans].UpperStateRef]
                        lower_state = result.data[
                                'States']["%s" % result.data[
                                    'RadiativeTransitions'][
                                        trans].LowerStateRef]
                    except (KeyError, AttributeError):
                        print(" -- Error: State is missing")
                        species_with_error.append(id)
                        continue

                    if id in result.data['Atoms'].keys():
                        is_atom = True
                        atomname = self.createatomname(
                                result.data['Atoms'][id])
                    elif id in result.data['Molecules'].keys():
                        is_atom = False
                        formula = str(result.data['Molecules'][
                            id].OrdinaryStructuralFormula)

                        # Get string which identifies the vibrational states
                        # involved in the transition
                        t_state = self.getvibstatelabel(
                                upper_state,
                                lower_state)

                    else:
                        continue

                    # Get hyperfinestructure info if hfsInfo is None only then
                    # the hfsInfo has not been inserted in the species name
                    # (there can be multiple values in the complete dataset
                    t_hfs = ''
                    try:
                        for pc in result.data['RadiativeTransitions'][
                                trans].ProcessClass:
                            if str(pc)[:3] == 'hyp':
                                t_hfs = str(pc)
                    except Exception as e:
                        print("Error: %s", e)

                    frequency = float(result.data['RadiativeTransitions']
                                      [trans].FrequencyValue)
                    try:
                        uncertainty = (
                            "%lf" % float(result.data['RadiativeTransitions']
                                          [trans].FrequencyAccuracy))
                    except TypeError:
                        print(" -- Error uncertainty not available")
                        species_with_error.append(id)
                        continue

                    # Get statistical weight if present
                    try:
                        weight = int(upper_state.TotalStatisticalWeight)
                    except Exception:
                        print(" -- Error statistical weight not available")
                        species_with_error.append(id)
                        continue

                    # Get nuclear spin isomer (ortho/para) if present
                    try:
                        nsi_name = upper_state.NuclearSpinIsomerName
                    except AttributeError:
                        nsi_name = None

                    # if nuclear spin isomer is defined then two entries have
                    # to be generated
                    if nsi_name is not None and nsi_name != '':
                        nsinames = [nsi_name, None]
                        nsi_state_origin = result.data['States'][
                                "%s"
                                % upper_state.NuclearSpinIsomerLowestEnergy]
                        nsi_energy_offset = float(
                                nsi_state_origin.StateEnergyValue)
                    else:
                        nsinames = [None]

                    for nsi in nsinames:
                        # create name
                        if is_atom:
                            t_name = atomname
                        else:
                            t_affix = ";".join(
                                    [affix for affix in
                                        [t_hfs, nsi]
                                        if affix is not None and affix != ''])

                            t_name = "%s;%s;%s" % (formula, t_state, t_affix)
                        t_name = t_name.strip()

                        # remove all blanks in the name
                        t_name = t_name.replace(' ', '')
                        # check if name is in the list of forbidden names and
                        # add counter if so
                        i = 1
                        while t_name in names_black_list:
                            t_name = "%s#%d" % (t_name.split('#')[0], i)
                            i += 1
                        # update list of distinct species names.
                        if id in species_names:
                            if t_name not in species_names[id]:
                                species_names[id].append(t_name)
                                num_transitions[t_name] = 0
                        else:
                            species_names[id] = [t_name]
                            num_transitions[t_name] = 0

                        if nsi is not None:
                            lower_state_energy = \
                                    float(lower_state.StateEnergyValue) \
                                    - nsi_energy_offset
                        else:
                            lower_state_energy = \
                                    float(lower_state.StateEnergyValue)

                        # Insert transition into database
                        try:
                            cursor.execute(
                                """INSERT INTO Transitions (
                                    T_Name,
                                    T_Frequency,
                                    T_EinsteinA,
                                    T_Uncertainty,
                                    T_EnergyLower,
                                    T_UpperStateDegeneracy,
                                    T_HFS,
                                    T_UpperStateQuantumNumbers,
                                    T_LowerStateQuantumNumbers) VALUES
                                    (?, ?,?,?,?, ?,?, ?,?)""",
                                (t_name,
                                 "%lf" % frequency,
                                 "%g" % float(result.data[
                                     'RadiativeTransitions'][
                                         trans].TransitionProbabilityA),
                                 uncertainty, "%lf" % lower_state_energy,
                                 weight,
                                 t_hfs,
                                 str(upper_state.QuantumNumbers.qn_string),
                                 str(lower_state.QuantumNumbers.qn_string),
                                 ))
                            num_transitions[t_name] += 1
                        except Exception as e:
                            print("Transition has not been inserted:\n"
                                  "Error: %s" % e)
            print("\n")
            # ------------------------------------------------------------------

            # ------------------------------------------------------------------
            # delete transitions for all entries where an error occured during
            # the insert
            for id in species_with_error:
                print(" -- Species {id} has not been inserted due to an "
                      "error ".format(id=str(id)))
                try:
                    for name in species_names[id]:
                        cursor.execute("DELETE FROM Transitions "
                                       "WHERE T_Name=?",
                                       (str(name),))
                        print(" --    {name} ".format(name=str(name)))
                except Exception as e:
                    print("Exception occured while removing incomplete "
                          "entries: %s" % str(e))

            # -----------------------------------------------------------------
            # insert specie in Partitionfunctions (header) table
            if node:
                resource_id = node.identifier
                url = node.url
            else:
                resource_id = 'NULL'
                url = 'NULL'

            # Insert molecules
            for id in species_names:
                if id in species_with_error:
                    continue
                for name in species_names[id]:
                    # determine hyperfine-structure affix and nuclear spin
                    # isomer affix
                    try:
                        hfs = ''
                        nsi = ''
                        for affix in name.split("#")[0].split(
                                ';', 2)[2].split(";"):
                            if affix.strip()[:3] == 'hyp':
                                hfs = affix.strip()
                            else:
                                # if affix does not identify hyperfine
                                # structure it identifies the nuclear spin
                                # isomer
                                nsi = affix.strip()
                    except Exception:
                        hfs = ''

                    # Insert row in partitionfunctions
                    try:
                        if id in result.data['Atoms']:
                            if 'Comment' not in result.data[
                                    'Atoms'][id].__dict__:
                                result.data['Atoms'][id].Comment = ""
                            cursor.execute("INSERT INTO Partitionfunctions "
                                           "(PF_Name, PF_SpeciesID, "
                                           " PF_VamdcSpeciesID, PF_Comment, "
                                           " PF_ResourceID, PF_URL, "
                                           " PF_Checkdate) VALUES "
                                           "(?,?,?,?,?,?,?)",
                                           ("%s" % name,
                                            id,
                                            "%s" % (result.data['Atoms'][id]
                                                    .VAMDCSpeciesID),
                                            "%s" % (result.data['Atoms'][id]
                                                    .Comment),
                                            resource_id,
                                            "%s%s%s" % (url, URL_STRING, id),
                                            datetime.now(), ))
                        else:
                            cursor.execute("INSERT INTO Partitionfunctions "
                                           "(PF_Name, PF_SpeciesID, "
                                           " PF_VamdcSpeciesID, PF_HFS, "
                                           " PF_NuclearSpinIsomer, PF_Comment,"
                                           " PF_ResourceID, PF_URL, "
                                           " PF_Checkdate) "
                                           "VALUES (?,?,?,?,?,?,?,?,?)",
                                           ("%s" % name,
                                            id,
                                            "%s" % (result.data['Molecules']
                                                    [id].VAMDCSpeciesID),
                                            hfs,
                                            nsi,
                                            "%s" % (result.data['Molecules']
                                                    [id].Comment),
                                            resource_id,
                                            "%s%s%s" % (url, URL_STRING, id),
                                            datetime.now(), ))
                    except sqlite3.Error as e:
                        print("An error occurred:", e.args[0])
                    except Exception as e:
                        print("An error occurred:", e.args[0])
                        print(list(result.data['Molecules'].keys()))

                # Update Partitionfunctions
                self.parse_and_update_partitionfunctions(id, result)

            for row in num_transitions:
                print("      for %s inserted %d transitions"
                      % (row, num_transitions[row]))
            self.conn.commit()
            cursor.close()

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

    def update_database(
            self,
            add_nodes=None,
            insert_only=False,
            update_only=False,
            delete_archived=False):
        """
        Checks if there are
        updates available for all entries. Updates will be retrieved from
        the resource specified in the database.  All resources will be
        searched for new entries, which will be inserted if available.
        Additional resources can be specified via add_nodes.

        :ivar nodes.Node add_nodes: Single or List of node-instances.
        :ivar boolean insert_only: Insert new species and skip updates
        :ivar boolean update_only: Updates species and skip inserts
        """
        # counter to identify which entry is currently processed
        counter = 0
        # counter to count available updates
        count_updates = 0
        # list of database - nodes which are currently in the local database
        dbnodes = []
        # create an instance with all available vamdc-nodes
        nl = nodes.Nodelist()

        # attach additional nodes to the list of dbnodes (for insert)
        if not functions.isiterable(add_nodes):
            add_nodes = [add_nodes]

        for node in add_nodes:
            if node is None:
                pass
            elif not isinstance(node, nodes.Node):
                print("Could not attach node. Wrong type, "
                      "it should be type <nodes.Node>")
            else:
                dbnodes.append(node)

        # --------------------------------------------------------------------
        # Check if updates are available for entries

        # Get list of species in the database
        cursor = self.conn.cursor()
        cursor.execute("SELECT PF_Name, PF_SpeciesID, PF_VamdcSpeciesID, "
                       "   datetime(PF_Checkdate), PF_ResourceID "
                       "FROM Partitionfunctions ")

        rows = cursor.fetchall()
        num_rows = len(rows)
        request = r.Request()

        if not insert_only:

            print("----------------------------------------------------------")
            print("Looking for updates")
            print("----------------------------------------------------------")

            for row in rows:
                counter += 1
                print("%5d/%5d: Check specie %-55s (%-15s): "
                      % (counter, num_rows, row[0], row[1]), end=' ')
                try:
                    node = nl.getnode(str(row[4]))
                except Exception:
                    node = None

                if node is None:
                    print(" -- RESOURCE NOT AVAILABLE")
                    continue
                else:
                    if node not in dbnodes:
                        dbnodes.append(node)

                # Currently the database prefix XCDMS- or XJPL- has to be
                # removed
                speciesid = row[1].split("-")[1]
                query_string = "SELECT ALL WHERE SpeciesID=%s"\
                               % speciesid
                request.setnode(node)
                request.setquery(query_string)

                errorcode = None
                try:
                    changedate = request.getlastmodified()
                except r.NoContentError:
                    # Delete entries which are not available anymore
                    if request.status == 204:
                        if delete_archived:
                            print(" --  ENTRY ARCHIVED AND WILL BE DELETED --")

                            del_specie = self.delete_species(row[1])

                            if len(del_specie) > 0:
                                print("\r Done")
                            else:
                                print(" -- ENTRY ARCHIVED -- ")
                    continue

                except r.TimeOutError:
                    print(" -- TIMEOUT: Could not check entry -- ")
                    continue

                except Exception:
                    changedate = None
                    print("Could not retrieve information - "
                          "Unexpected error:", sys.exc_info()[0])
                    continue

                tstamp = parser.parse(row[3] + " GMT")
                if changedate is None:
                    if errorcode is None:
                        errorcode = "UNKNOWN"
                        print(" -- %s (Could not retrieve information)"
                              % errorcode)
                    continue
                if tstamp < changedate:
                    print(" -- UPDATE AVAILABLE ")

                    count_updates += 1
                    print(" -- PERFORM UPDATE -- ")
                    query_string = ("SELECT SPECIES WHERE SpeciesID=%s"
                                    % speciesid)
                    request.setquery(query_string)

                    result = request.dorequest()
                    try:
                        result.populate_model()
                    except Exception:
                        print(" Error: Could not process data ")
                        continue
                    try:
                        self.insert_species_data(
                                result.data['Molecules'],
                                node,
                                update=True)
                    except Exception:
                        print(" Error: Could not update data ")
                        continue
                    print(" -- UPDATE DONE    -- ")
                else:
                    print(" -- up to date")

            if count_updates == 0:
                print("\r No updates for your entries available")
            print("Done")
        else:
            cursor.execute("SELECT distinct PF_ResourceID "
                           "FROM Partitionfunctions ")
            rows = cursor.fetchall()
            for row in rows:
                try:
                    node = nl.getnode(str(row[0]))
                except Exception:
                    node = None
                if node is None:
                    print(" -- RESOURCE NOT AVAILABLE")
                    continue
                else:
                    if node not in dbnodes:
                        dbnodes.append(node)

        if update_only:
            return

        # Check if there are new entries available

        # ---------------------------------------------------------
        # Check all dbnodes for new species
        for node in dbnodes:
            counter = 0
            insert_molecules_list = []
            print("----------------------------------------------------------")
            print("Query '{dbname}' for new species ".format(dbname=node.name))
            print("----------------------------------------------------------")
            request.setnode(node)
            result = request.getspecies()
            for id in result.data['Molecules']:
                try:
                    cursor.execute("SELECT PF_Name, PF_SpeciesID, "
                                   " PF_VamdcSpeciesID, PF_Checkdate  "
                                   "FROM Partitionfunctions "
                                   "WHERE PF_SpeciesID=?", [(id)])
                    exist = cursor.fetchone()
                    if exist is None:
                        print("   %s" % result.data['Molecules'][id])
                        insert_molecules_list.append(
                                result.data['Molecules'][id])
                        counter += 1
                except Exception as e:
                    print(e)
                    print(id)
            print("There are %d new species available" % counter)
            print("----------------------------------------------------------")
            print("Start insert")
            print("----------------------------------------------------------")
            self.insert_species_data(insert_molecules_list, node)
            print("----------------------------------------------------------")
            print("Done")

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
