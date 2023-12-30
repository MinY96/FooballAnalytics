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
            return True
        except Exception as err:
            print(f"DB 접속 에러 : {err}")
            return False
            
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
            return True
        except Exception as err:
            print(f"DB 접속 에러 : {err}")
            return False
    
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
            
    def createDB(self, db_name):
        """
        Database 생성

        Args:
            db_name (str): 생성할 DB명
        """
        
        global conn, cur
        if (self.connect()):
            query = f"CREATE DATABASE {db_name}"
            cur.execute(query)
            conn.commit()      
            self.disconnect()      
        


# db = Postgres('localhost', 'postgres', 5432, 'udmt')
db = Postgres('localhost', 'postgres', 5432, 'alsdud12')
db.createDB('football')