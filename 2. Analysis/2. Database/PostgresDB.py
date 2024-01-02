import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

class Postgres():
    
    conn = None
    cur = None
    
    def __init__(self, host, user, port, pw):
        self.host = host
        self.user = user
        self.port = port
        self.pw = pw
        
    def connect(self):
        """
        postgresql 연결
        """
        global conn, cur
        try:
            # postgresql 연결
            conn = psycopg2.connect(host=self.host, user=self.user, port=self.port, password=self.pw)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            # cursor 객체 생성
            cur = conn.cursor()
        except Exception as err:
            print(f"DB 접속 에러 : {err}")
            
    def connectDB(self, db_name):
        """
        postgresql Database 연결
        Args:
            db_name (str): 접속할 DB명
        """
        global conn, cur
        try:
            # postgresql db 연결
            conn = psycopg2.connect(host=self.host, user=self.user, port=self.port, password=self.pw, database=db_name)
            # cursor 객체 생성
            cur = conn.cursor()
        except Exception as err:
            print(f"DB 접속 에러 : {err}")
    
    def disconnect(self):
        """
        postgresql 연결 해제
        """
        global conn, cur
        try:
            # postgresql 연결 해재
            conn.close()
            # cursor 객체 해제
            cur.close()
        except Exception as err:
            print('postgresql 서버에 연결되어 있지 않습니다')
            
            
    def existDB(self, db_name):
        """
        Database 존재 여부 확인

        Args:
            db_name (str): 조회할 DB명
        """
        global conn, cur
        exist = False
        try:
            self.connect()
            query = "SELECT datname FROM pg_catalog.pg_database "+\
                f"WHERE datname = '{db_name}'"
            cur.execute(query)
            row = cur.fetchall()
            if len(row)>0:
                exist = True
        except Exception as err:
            print(err)
        finally:
            self.disconnect()
            return exist
            
            
    def createDB(self, db_name):
        """
        Database 생성

        Args:
            db_name (str): 생성할 DB명
        """
        
        global conn, cur
        try:
            self.connect()
            query = f"CREATE DATABASE {db_name}"
            cur.execute(query)
            conn.commit()      
        except Exception as err:
            print(err)
        finally:
            self.disconnect()
            
    
    def selectTables(self, db_name):
        """
        Database 내 테이블 목록 조회

        Args:
            db_name (str): 테이블 목록 조회할 Database
        """
        
        global conn, cur
        try:
            self.connectDB(db_name)
            query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "+\
                "WHERE TABLE_SCHEMA = 'public'"
            cur.execute(query)
            rows = cur.fetchall()
            return [row[0] for row in rows] if len(rows)>0 else []
        except Exception as err:
            print(err)
        finally:
            self.disconnect()
        
    def createTable(self, db_name, tbl_name, cols):
        """
        Table 생성

        Args:
            db_name (str): 테이블 생성될 Database
            tbl_name (str): 생성할 테이블 명
            cols (list): (column 이름, column 타입)으로 구성된 리스트
        """
        
        global conn, cur
        try:
            self.connectDB(db_name)
            query_col = f"({','.join([f'{col[0]} {col[1]}' for col in cols])})"
            query = f"CREATE TABLE {tbl_name} {query_col}"
            cur.execute(query)
            conn.commit()      
        except Exception as err:
            print(err)
        finally:
            self.disconnect()


dic_table = {
    'tbl_league':[('nation','varchar(50)'),
                ('league','varchar(50)')],
    'tbl_team':[('team','varchar(50)'),
                ('founded','varchar(50)'),
                ('stadium','varchar(50)'),
                ('seat','varchar(50)')],
    'tbl_season':[('league','varchar(50)'),
                ('season','varchar(50)'),
                ('team','varchar(50)'),
                ('manager','varchar(50)'),
                ('captain','varchar(50)')],
    'tbl_season_position':[('league','varchar(50)'),
                ('season','varchar(50)'),
                ('team','varchar(50)'),
                ('point','integer'),
                ('won','integer'),
                ('drawn','integer'),
                ('lost','integer'),
                ('gd','integer'),
                ('gf','integer'),
                ('ga','integer'),
                ('yellow','integer'),
                ('red','integer')],
    'tbl_season_match':[('league','varchar(50)'),
                ('season','varchar(50)'),
                ('team','varchar(50)'),
                ('referee','varchar(50)'),
                ('opp','varchar(50)'),
                ('h_a','varchar(50)'),
                ('last5','varchar(50)'),
                ('result','integer'),
                ('point','integer'),
                ('gd','integer'),
                ('gf','integer'),
                ('ga','integer'),
                ('fhgf','integer'),
                ('fhga','integer'),
                ('shgf','integer'),
                ('shga','integer'),
                ('shot','integer'),
                ('sho_on_target','integer'),
                ('corner','integer'),
                ('foul','integer'),
                ('yellow','integer'),
                ('red','integer'),
                ('acc_point','integer'),
                ('acc_gd','integer'),
                ('acc_gf','integer'),
                ('position','integer')],
}

    
db = Postgres('localhost', 'postgres', 5432, 'udmt')
db_name = 'football'
if not db.existDB(db_name):
    db.createDB(db_name)
    
tbls = db.selectTables(db_name)
for tbl, cols in dic_table.items():
    if tbl not in tbls:
        print(f'테이블 생성 : {tbl}')
        db.createTable(db_name, tbl, cols)