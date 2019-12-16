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
    from . import query as q
    from . import results
    from . import request as r
    from . import nodes
    from . import specmodel
    from . import settings
else:
    import functions
    import query as q
    import results
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

    def db_insert_partitionfunction(self, pfrow):
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

        self.conn.commit()

        cursor.execute("SELECT last_insert_rowid();")
        rowid = cursor.fetchone()[0]

        cursor.close()

        return rowid

    def check_for_updates(self, node):
        """
        Checks for each database entry if an update for the molecular or atomic
        specie is available in the specified VAMDC database node.

        :ivar nodes.Node node: VAMDC database node which will be checked
                               for updates
        """
        count_updates = 0
        counter = 0
        cursor = self.conn.cursor()
        cursor.execute("SELECT PF_Name, PF_SpeciesID, PF_VamdcSpeciesID, \
                       datetime(PF_Checkdate) FROM Partitionfunctions ")
        rows = cursor.fetchall()
        num_rows = len(rows)
        query = q.Query()
        request = r.Request()

        for row in rows:
            counter += 1
            print("%5d/%5d: Check specie %-55s (%-15s): "
                  % (counter, num_rows, row[0], row[1]), end=' ')
            vamdcspeciesid = row[2]
            query_string = "SELECT ALL WHERE SpeciesID=%s" % row[1][6:]
            request.setquery(query_string)
            request.setnode(node)

            try:
                changedate = request.getlastmodified()
            except r.TimeOutError:
                print("TIMEOUT")
                continue
            except r.NoContentError:
                print("ENTRY OUTDATED")
                changedate = None
                continue
            except Exception as e:
                print("Error in getlastmodified: %s " % str(e))
                print("Status - code: %s" % str(request.status))
                changedate = None
                continue

            tstamp = parser.parse(row[3] + " GMT")
            if changedate is None:
                print(" -- UNKNOWN (Could not retrieve information)")
                continue
            if tstamp < changedate:
                print(" -- UPDATE AVAILABLE ")
                count_updates += 1
            else:
                print(" -- up to date")

        if count_updates == 0:
            print("\r No updates for your entries available")
        print("Done")

    def check_for_new_species(self, node):
        """
        Checks for new entries in the VAMDC database node which are not
        available in the local sqlite3 database.

        :ivar nodes.Node node: VAMDC database node which will be checked
                               for updates
        """

        counter = 0
        cursor = self.conn.cursor()

        # Try to identify node if only specified by a string
        if type(node) == str:
            nl = nodes.Nodelist()
            node = nl.findnode(node)

        species = r.getspecies(node=node)

        print("----------------------------------------------------------")
        print("Query '{dbname}' for new species ".format(dbname=node.name))
        print("----------------------------------------------------------")

        for id in species['Molecules'].keys() + species['Atoms'].keys():
            try:
                cursor.execute("SELECT PF_ID FROM Partitionfunctions "
                               "WHERE PF_SpeciesID=?", [(id)])
                exist = cursor.fetchone()
                if exist is None:
                    pfrow = PFrow(species_id=id)
                    if id in species['Atoms'].keys():
                        print("ID: %s" % species['Atoms'][id])
                        name = self.createatomname(species['Atoms'][id])
                        pfrow.name = name
                        pfrow.stoichiometricformula = name
                        pfrow.ordinarystructuralformula = name
                        pfrow.chemicalname = name

                        if 'Comment' not in species['Atoms'][id].__dict__:
                            pfrow.comment = ""
                        else:
                            pfrow.comment = species['Atoms'][id].Comment

                        pfrow.vamdc_species_id = \
                            "%s" % (species['Atoms'][id].VAMDCSpeciesID)
                    else:
                        print("ID: %s" % species['Molecules'][id])
                        formula = str(species['Molecules'][
                            id].OrdinaryStructuralFormula)
                        pfrow.ordinarystructuralformula = formula
                        pfrow.stoichiometricformula = str(
                                species['Molecules'][id].StoichiometricFormula
                            ).strip()
                        pfrow.chemicalname = str(
                                species['Molecules'][id].ChemicalName).strip()
                        pfrow.name = formula
                        pfrow.comment = \
                            "%s" % (species['Molecules'][id].Comment)
                        pfrow.vamdc_species_id = \
                            "%s" % (species['Molecules'][id].VAMDCSpeciesID)

                    pfrow.resource_id = str(node.identifier)

                    # insert new row into database with status -> 'new'
                    self.db_insert_partitionfunction(pfrow)
                    print("ID: %s" % result.data['Molecules'][id])
                    counter += 1
            except Exception as e:
                print("Exception occured while checking updates "
                      "for species %d:\n %s" % (id, e))

        print("There are %d new species available" % counter)

    def show_species(self):
        """
        Lists all species, which are stored in the local sqlite3 database.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT PF_Name, PF_SpeciesID, PF_VamdcSpeciesID, \
                        PF_Recommendation, PF_Status, PF_Createdate, \
                        PF_Checkdate FROM Partitionfunctions")
        rows = cursor.fetchall()
        for row in rows:
            print("%-10s %-60s %20s %10s %10s %s %s" %
                  (row[1], row[0], row[2], row[3], row[4], row[5], row[6]))

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
            resourceID = node.identifier
            url = node.url
        else:
            resourceID = 'NULL'
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
                    "  AND PF_ResourceID='{rid}'".format(
                            si=vamdcspeciesid,
                            rid=resourceID))
                print("SELECT PF_ID FROM Partitionfunctions "
                      "WHERE PF_VamdcSpeciesID='{si}' "
                      "  AND PF_ResourceID='{rid}'".format(
                          si=vamdcspeciesid,
                          rid=resourceID))

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
                        is_molecule = False
                        atomname = self.createatomname(
                                result.data['Atoms'][id]).strip()
                    elif id in result.data['Molecules'].keys():
                        is_atom = False
                        is_molecule = True
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
                        nsiName = upper_state.NuclearSpinIsomerName.strip()
                    except AttributeError:
                        nsiName = None

                    # if nuclear spin isomer is defined then two entries have
                    # to be generated
                    if nsiName is not None and nsiName != '':
                        nsinames = [nsiName, None]
                        nsiStateOrigin = result.data['States'][
                            "%s" % upper_state.NuclearSpinIsomerLowestEnergy]
                        nsiEnergyOffset = float(
                                nsiStateOrigin.StateEnergyValue)
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
                            lowerStateEnergy = \
                                    float(lower_state.StateEnergyValue) \
                                    - nsiEnergyOffset
                        else:
                            lowerStateEnergy = \
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
                                        (result.data['Atoms'][id].ChemicalElementSymbol
                                         + result.data['Atoms'][id].ChemicalElementNuclearCharge)

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
                                         "%s" % (result.data['Atoms'][id].VAMDCSpeciesID),
                                         formula,
                                         formula,
                                         "%s" % (result.data['Atoms'][id].Comment),
                                         resourceID,
                                         "%s%s%s" % (url, "sync?LANG=VSS2&amp;REQUEST=doQuery&amp;FORMAT=XSAMS&amp;QUERY=Select+*+where+SpeciesID%3D", id),
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
                                         "%s" % (result.data['Molecules'][id].VAMDCSpeciesID),
                                         "%s" % (result.data['Molecules'][id].StoichiometricFormula),
                                         "%s" % (result.data['Molecules'][id].OrdinaryStructuralFormula),
                                         "%s" % (result.data['Molecules'][id].ChemicalName),
                                         t_hfs,
                                         nsiName,
                                         t_state,
                                         "%s" % (result.data['Molecules'][id].Comment),
                                         resourceID,
                                         "%s%s%s" % (url, "sync?LANG=VSS2&amp;REQUEST=doQuery&amp;FORMAT=XSAMS&amp;QUERY=Select+*+where+SpeciesID%3D", id),
                                         datetime.now(), ))

                                db_id = cursor.lastrowid
                                species_dict_id[(id, nsiName, t_hfs, t_state)] = db_id
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
                                  float(result.data['RadiativeTransitions'][trans].TransitionProbabilityA),
                                  uncertainty, "%lf" % lowerStateEnergy,
                                  weight,
                                  t_hfs,
                                  str(upper_state.QuantumNumbers.qn_string),
                                  str(lower_state.QuantumNumbers.qn_string),
                                 ))
                            num_transitions[t_name] += 1
                        except Exception as e:
                            print("Transition has not been inserted:\n Error: %s" % e)
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

                if (result.data['RadiativeTransitions'][trans].SpeciesID
                        == speciesid or speciesid is None):
                    id = str(result.data['RadiativeTransitions']
                             [trans].SpeciesID)

                    # if an error has occured already then there will be no
                    # further insert
                    if id in species_with_error:
                        continue

                    # Get upper and lower state from the states table
                    try:
                        upper_state = result.data['States']["%s"
                                % result.data['RadiativeTransitions'][trans].UpperStateRef]
                        lower_state = result.data['States']["%s"
                                % result.data['RadiativeTransitions'][trans].LowerStateRef]
                    except (KeyError, AttributeError):
                        print(" -- Error: State is missing")
                        species_with_error.append(id)
                        continue

                    if id in result.data['Atoms'].keys():
                        is_atom = True
                        is_molecule = False
                        atomname = self.createatomname(
                                result.data['Atoms'][id])
                    elif id in result.data['Molecules'].keys():
                        is_atom = False
                        is_molecule = True
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
                        for pc in result.data['RadiativeTransitions'][trans].ProcessClass:
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
                        nsiName = upper_state.NuclearSpinIsomerName
                    except AttributeError:
                        nsiName = None

                    # if nuclear spin isomer is defined then two entries have
                    # to be generated
                    if nsiName is not None and nsiName != '':
                        nsinames = [nsiName, None]
                        nsiStateOrigin = result.data['States'][
                                "%s"
                                % upper_state.NuclearSpinIsomerLowestEnergy]
                        nsiEnergyOffset = float(
                                nsiStateOrigin.StateEnergyValue)
                    else:
                        nsinames = [None]

                    for nsiName in nsinames:
                        # create name
                        if is_atom:
                            t_name = atomname
                        else:
                            t_affix = ";".join(
                                    [affix for affix in
                                        [t_hfs, nsiName]
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

                        if nsiName is not None:
                            lowerStateEnergy = \
                                    float(lower_state.StateEnergyValue) \
                                    - nsiEnergyOffset
                        else:
                            lowerStateEnergy = \
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
                                 uncertainty, "%lf" % lowerStateEnergy,
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
                resourceID = node.identifier
                url = node.url
            else:
                resourceID = 'NULL'
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
                        for affix in name.split("#")[0].split(';', 2)[2].split(";"):
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
                            if 'Comment' not in result.data['Atoms'][id].__dict__:
                                result.data['Atoms'][id].Comment = ""
                            cursor.execute("INSERT INTO Partitionfunctions \
                                           (PF_Name, PF_SpeciesID, \
                                            PF_VamdcSpeciesID, PF_Comment, \
                                            PF_ResourceID, PF_URL, PF_Checkdate) VALUES (?,?,?,?,?,?,?)",
                                           ("%s" % name,
                                            id,
                                            "%s" % (result.data['Atoms'][id].VAMDCSpeciesID),
                                            "%s" % (result.data['Atoms'][id].Comment),
                                            resourceID,
                                            "%s%s%s" % (url, "sync?LANG=VSS2&amp;REQUEST=doQuery&amp;FORMAT=XSAMS&amp;QUERY=Select+*+where+SpeciesID%3D", id),
                                            datetime.now(), ))
                        else:
                            cursor.execute("INSERT INTO Partitionfunctions \
                                           (PF_Name, PF_SpeciesID, \
                                            PF_VamdcSpeciesID, PF_HFS, \
                                            PF_NuclearSpinIsomer, PF_Comment, \
                                            PF_ResourceID, PF_URL, PF_Checkdate) VALUES (?,?,?,?,?,?,?,?,?)",
                                           ("%s" % name,
                                            id,
                                            "%s" % (result.data['Molecules'][id].VAMDCSpeciesID),
                                            hfs,
                                            nsi,
                                            "%s" % (result.data['Molecules'][id].Comment),
                                            resourceID,
                                            "%s%s%s" % (url, "sync?LANG=VSS2&amp;REQUEST=doQuery&amp;FORMAT=XSAMS&amp;QUERY=Select+*+where+SpeciesID%3D", id),
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
        # Thus only partitionfunctions for molecules can be found in the result.
        # and others need to be calculated based on state energies.
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
                          "specie %d:\n%s " % (id, str(e)))

                self.update_partitionfunction(
                        id,
                        temperature,
                        pf_values[id])
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
        Update the partition function in the database for an atom/molecule
        for the given temperature

        :var id: species-id
        :type id: int
        :var temperature: temperature (the corresponding field has to
                          exist in the database
        :type temperature: float
        :par value: value of the partitionfunction
        :type value: float
        :par nsi: nuclear spin identifier
        :type nsi. str
        """
        try:
            field = ("PF_%.3lf" % float(temperature)).replace('.', '_')
            sql = ("UPDATE Partitionfunctions "
                   "SET %s=? WHERE PF_SpeciesID=? "
                   "AND IFNULL(PF_NuclearSpinIsomer,'')=?" % field)

            self.conn.cursor.execute(sql, value, id, nsi)
        except Exception as e:
            print("SQL-Error occred while updating partitionfunction for "
                  "species-id %d and temperature %lf\nsql: %s "
                  % (id, temperature, sql))
            print("Error: %d: %s" % (e.args[0], e.args[1]))

    # ************************************************************************
    def update_database(self,
                        add_nodes=None,
                        insert_only=False,
                        update_only=False,
                        delete_archived=False):
        """
        Checks if there are updates available for all entries. Updates will
        be retrieved from the resource specified in the database.
        All resources will be searched for new entries, which will be inserted
        if available. Additional resources can be specified via add_nodes.

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
        query = q.Query()
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

                vamdcspeciesid = row[2]
                # Currently the database prefix XCDMS- or XJPL- has to be
                # removed
                speciesid = row[1].split("-")[1]
                query_string = "SELECT ALL WHERE SpeciesID=%s" % speciesid
                request.setnode(node)
                request.setquery(query_string)

                errorcode = None
                try:
                    changedate = request.getlastmodified()
                except r.NoContentError:
                    # Delete entries which are not available anymore
                    if request.status == 204:
                        if delete_archived:
                            print(" -- ENTRY ARCHIVED AND WILL BE DELETED -- ")
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
                    print("Could not retrieve information - Unexpected error:",
                          sys.exc_info()[0])
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
                                   "FROM Partitionfunctions WHERE PF_SpeciesID=?",
                                   [(id)])
                    exist = cursor.fetchone()
                    if exist is None:
                        print("   %s" % result.data['Molecules'][id])
                        insert_molecules_list.append(result.data['Molecules'][id])
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
        if (upper_state.QuantumNumbers.vibstate
                == lower_state.QuantumNumbers.vibstate):
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

        return t_state

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

        symbol = atom.ChemicalElementSymbol

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

        return "%s%s%s" % (massnumber, atom.ChemicalElementSymbol, charge_str)
