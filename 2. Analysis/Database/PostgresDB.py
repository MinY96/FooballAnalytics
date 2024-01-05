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
            
    def connectDB(self, db_nm):
        """
        postgresql Database 연결
        Args:
            db_nm (str): 접속할 DB명
        """
        global conn, cur
        try:
            # postgresql db 연결
            conn = psycopg2.connect(host=self.host, user=self.user, port=self.port, password=self.pw, database=db_nm)
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
            
            
    def existDB(self, db_nm):
        """
        Database 존재 여부 확인

        Args:
            db_nm (str): 조회할 DB명
        """
        global conn, cur
        exist = False
        try:
            self.connect()
            query = "SELECT datname FROM pg_catalog.pg_database "+\
                f"WHERE datname = '{db_nm}'"
            cur.execute(query)
            row = cur.fetchall()
            if len(row)>0:
                exist = True
        except Exception as err:
            print(err)
        finally:
            self.disconnect()
            return exist
            
            
    def createDB(self, db_nm):
        """
        Database 생성

        Args:
            db_nm (str): 생성할 DB명
        """
        
        global conn, cur
        try:
            self.connect()
            query = f"CREATE DATABASE {db_nm}"
            cur.execute(query)
            conn.commit()      
        except Exception as err:
            print(err)
        finally:
            self.disconnect()
            
            
    def dropDB(self, db_nm):
        """
        Database 제거

        Args:
            db_nm (str): Database 명
        """
        
        global conn, cur
        try:
            self.connect()
            query = f"DROP DATABASE IF EXISTS {db_nm}"
            cur.execute(query)
            conn.commit()
        except Exception as err:
            print(err)
        finally:
            self.disconnect()
            
    
    def selectTables(self, db_nm):
        """
        Database 내 테이블 목록 조회

        Args:
            db_nm (str): 테이블 목록 조회할 Database
        """
        
        global conn, cur
        try:
            self.connectDB(db_nm)
            query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "+\
                "WHERE TABLE_SCHEMA = 'public'"
            cur.execute(query)
            rows = cur.fetchall()
            return [row[0] for row in rows] if len(rows)>0 else []
        except Exception as err:
            print(err)
        finally:
            self.disconnect()
        
    def createTable(self, db_nm, tbl_nm, cols):
        """
        Table 생성

        Args:
            db_nm (str): 테이블 생성될 Database
            tbl_nm (str): 생성할 테이블 명
            cols (list): (column 이름, column 타입)으로 구성된 리스트
        """
        
        global conn, cur
        try:
            self.connectDB(db_nm)
            query_col = f"({','.join([f'{col[0]} {col[1]}' for col in cols])})"
            query = f"CREATE TABLE {tbl_nm} {query_col}"
            cur.execute(query)
            conn.commit()      
        except Exception as err:
            print(err)
        finally:
            self.disconnect()
            
    
    def dropTable(self, db_nm, tbl_nm):
        """
        Table 제거

        Args:
            db_nm (str): Database 명
            tbl_nm (str): Table 명
        """
        
        global conn, cur
        try:
            self.connectDB(db_nm)
            query = f"DROP TABLE IF EXISTS {tbl_nm}"
            cur.execute(query)
            conn.commit()
        except Exception as err:
            print(err)
        finally:
            self.disconnect()
            
            
    def executeDB(self, db_nm, query):
        """
        DB내 특정 query 실행

        Args:
            db_nm (str): Database 명
            query (str): 쿼리문
        """
        
        global conn, cur
        try:
            self.connectDB(db_nm)
            cur.execute(query)
            conn.commit()
        except Exception as err:
            print(err)
        finally:
            self.disconnect()
            
    def selectDB(self, db_nm, query):
        """
        DB 내 Select문 실행

        Args:
            db_nm (str): Database 명
            query (str): Select 쿼리문
        """
        
        global conn, cur
        rows = None
        try:
            self.connectDB(db_nm)
            cur.execute(query)
            rows = cur.fetchall()
        except Exception as err:
            print(err)
            rows = None
        finally:
            self.disconnect()
            return rows