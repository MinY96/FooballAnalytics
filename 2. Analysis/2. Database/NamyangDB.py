import psycopg2

class NamyangDB():

    def __init__(self):

        self.host = 'localhost'
        self.db = 'namyang'
        self.user = 'postgres'
        self.password = 'udmt'
        self.port = 5432
        self.tbl_tag_info = 'tbl_tag_info'
        self.tbl_cycle_info = 'tbl_cycle_info'
        self.tbl_lot_info = 'tbl_lot_info'
        self.tbl_cycle_log = 'tbl_cycle_log'
        self.tbl_lot_log = 'tbl_lot_log'
        self.tbl_timelog = 'tbl_timelog'

    def select(self, query):

        conn = None
        rows = None
        try:            
            with psycopg2.connect(host=self.host, dbname=self.db, user=self.user, password=self.password, port=self.port) as conn:                
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()           
        except Exception as e:
            print('Error : ', e)        
        finally:
            if conn:
                conn.close()
                return rows

    def create(self, query):

        conn = None        
        try:            
            with psycopg2.connect(host=self.host, dbname=self.db, user=self.user, password=self.password, port=self.port) as conn:                
                with conn.cursor() as cur:
                    cur.execute(query)                
        except Exception as e:
            print('Error : ', e)        
        finally:
            if conn:
                conn.close()

    def insert(self, query):

        conn = None        
        try:            
            with psycopg2.connect(host=self.host, dbname=self.db, user=self.user, password=self.password, port=self.port) as conn:                
                with conn.cursor() as cur:
                    cur.execute(query)                
        except Exception as e:
            print('Error : ', e)        
        finally:
            if conn:
                conn.commit()
                conn.close()


    def check_table(self):

        query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'public'"
        tables = self.select(query)
        ls_tbl = [table[0] for table in tables]
        return ls_tbl

    def create_tbl_tag_info(self):

        query = "CREATE TABLE {} ".format(self.tbl_tag_info) +\
                "(tag_id integer NOT NULL, " +\
                "process_nm_kr varchar(50) NOT NULL, " +\
                "process_nm_en varchar(50) NOT NULL, " +\
                "tag_nm varchar(50) NOT NULL, " +\
                "tag_nm_kr varchar(50) NOT NULL, " +\
                "unit varchar(50) NOT NULL, " +\
                "feature varchar(50) NOT NULL, " +\
                "PRIMARY KEY (tag_id) );"
        self.create(query)

    def create_tbl_cycle_info(self):

        query = "CREATE TABLE {} ".format(self.tbl_cycle_info) +\
                "(cycle_id integer NOT NULL, " +\
                "dt_start timestamp NOT NULL, " +\
                "dt_end timestamp NOT NULL, " +\
                "cycle_tm double precision NOT NULL, " +\
                "ok_ng boolean NOT NULL, " +\
                "PRIMARY KEY (cycle_id));"
        self.create(query)

    def create_tbl_lot_info(self):

        query = "CREATE TABLE {} ".format(self.tbl_lot_info) +\
                "(lot_id integer NOT NULL, " +\
                "dt_start timestamp NOT NULL, " +\
                "dt_end timestamp NULL, " +\
                "mold_nm varchar(50) NOT NULL, " +\
                "lot_nm varchar(50) NOT NULL, " +\
                "prod_cnt integer NULL, " +\
                "ng_cnt integer NULL, " +\
                "PRIMARY KEY (lot_id));"    
        self.create(query)

    def create_tbl_cycle_log(self):

        query = "CREATE TABLE {} ".format(self.tbl_cycle_log) +\
                "(lot_id integer NOT NULL, " +\
                "cycle_id integer NOT NULL, " +\
                "tag_id integer NOT NULL, " +\
                "ftr_val double precision NOT NULL); " +\
                "ALTER TABLE {} ".format(self.tbl_cycle_log) +\
                "ADD CONSTRAINT FK_{}_cycle_id_{}_cycle_id FOREIGN KEY (cycle_id) ".format(self.tbl_cycle_log, self.tbl_cycle_info) +\
                "REFERENCES {} (cycle_id) ON DELETE CASCADE ON UPDATE CASCADE;".format(self.tbl_cycle_info) +\
                "ALTER TABLE {} ".format(self.tbl_cycle_log) +\
                "ADD CONSTRAINT FK_{}_tag_id_{}_tag_id FOREIGN KEY (tag_id) ".format(self.tbl_cycle_log, self.tbl_tag_info) +\
                "REFERENCES {} (tag_id) ON DELETE CASCADE ON UPDATE CASCADE; ".format(self.tbl_tag_info) +\
                "CREATE INDEX index_tbl_cycle_log_cycle_tag_id ON {} (cycle_id, tag_id)".format(self.tbl_cycle_log)
        self.create(query)

    def create_tbl_lot_log(self):

        query = "CREATE TABLE {} ".format(self.tbl_lot_log) +\
                "(lot_id integer NOT NULL, " +\
                "tag_id integer NOT NULL, " +\
                "low_val double precision NOT NULL, " +\
                "up_val double precision NOT NULL, " +\
                "low_exd_cnt integer NOT NULL, " +\
                "up_exd_cnt integer NOT NULL, " +\
                "ftr_val_mean double precision NOT NULL, " +\
                "ftr_val_min double precision NOT NULL, " +\
                "ftr_val_max double precision NOT NULL); " +\
                "ALTER TABLE {} ".format(self.tbl_lot_log) +\
                "ADD CONSTRAINT FK_{}_lot_id_{}_lot_id FOREIGN KEY (lot_id) ".format(self.tbl_lot_log, self.tbl_lot_info) +\
                "REFERENCES {} (lot_id) ON DELETE CASCADE ON UPDATE CASCADE;".format(self.tbl_lot_info) +\
                "ALTER TABLE {} ".format(self.tbl_lot_log) +\
                "ADD CONSTRAINT FK_{}_tag_id_{}_tag_id FOREIGN KEY (tag_id) ".format(self.tbl_lot_log, self.tbl_tag_info) +\
                "REFERENCES {} (tag_id) ON DELETE CASCADE ON UPDATE CASCADE;".format(self.tbl_tag_info) +\
                "CREATE INDEX index_tbl_lot_log_lot_tag_id ON {} (lot_id, tag_id)".format(self.tbl_lot_log)
        self.create(query)

    def create_tbl_timelog(self):

        query = "CREATE TABLE {} ".format(self.tbl_timelog) +\
                "(tag_id integer NOT NULL, " +\
                "value varchar(50) NOT NULL, " +\
                "dt_time timestamp NOT NULL); " +\
                "CREATE OR REPLACE FUNCTION add_task_notify() " +\
                "RETURNS trigger AS $BODY$ BEGIN " +\
                "PERFORM pg_notify('new_cycle', NEW::text); " +\
                "RETURN NEW; END; $BODY$ " +\
                "LANGUAGE plpgsql VOLATILE COST 100; " +\
                "ALTER FUNCTION add_task_notify() " +\
                "OWNER TO postgres; " +\
                "CREATE TRIGGER add_task_event_trigger " +\
                "AFTER INSERT ON {} ".format(self.tbl_timelog) +\
                "FOR EACH ROW " +\
                "WHEN (NEW.tag_id = 0) " +\
                "EXECUTE PROCEDURE add_task_notify();"
        self.create(query)

    def insert_tbl_tag_info(self):

        query = "INSERT INTO {} VALUES ".format(self.tbl_tag_info) +\
                "(0, '샷', 'shot_cnt', '', '', '', ''), " +\
                "(1, '로트정보', 'lot_nm', '', '', '', ''), "+\
                "(2, '금형정보', 'mold_nm', '', '', '', ''), "+\
                "(3, '호퍼', 'Hopper', 'HopperTmp', '온도', '℃', 'avg'), "+\
                "(4, '스크류', 'Screw', 'ScrewSpd', '속도', 'cm/s', 'avg'), "+\
                "(5, '배럴', 'Barrel', 'BarrelTmpNH', '온도NH', '℃', 'avg'), "+\
                "(6, '배럴', 'Barrel', 'BarrelTmpH1', '온도H1', '℃', 'avg'), "+\
                "(7, '배럴', 'Barrel', 'BarrelTmpH2', '온도H2', '℃', 'avg'), "+\
                "(8, '배럴', 'Barrel', 'BarrelTmpH3', '온도H3', '℃', 'avg'), "+\
                "(9, '노즐', 'Nozzle', 'NozzleSpd', '속도', 'cm/s', 'avg'), "+\
                "(10, '노즐', 'Nozzle', 'NozzlePrs', '압력', 'bar', 'avg'), "+\
                "(11, '금형', 'Mold', 'MoldTmpIn', '온도IN', '℃', 'avg'), "+\
                "(12, '금형', 'Mold', 'MoldTmpOut', '온도OUT', '℃', 'avg');"
        self.insert(query)

    def create_ny_table(self):

        ls_tbl = self.check_table()
        if self.tbl_tag_info not in ls_tbl:
            self.create_tbl_tag_info()
            self.insert_tbl_tag_info()
        if self.tbl_cycle_info not in ls_tbl:
            self.create_tbl_cycle_info()
        if self.tbl_lot_info not in ls_tbl:
            self.create_tbl_lot_info()
        if self.tbl_cycle_log not in ls_tbl:
            self.create_tbl_cycle_log()
        if self.tbl_lot_log not in ls_tbl:
            self.create_tbl_lot_log()
        if self.tbl_timelog not in ls_tbl:
            self.create_tbl_timelog()
