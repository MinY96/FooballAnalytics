from .PostgresDB import Postgres

dic_table = {
    'tbl_league':[('nation','varchar(50)'),
                ('league','varchar(50)')],
    'tbl_team':[('team','varchar(50) UNIQUE NOT NULL'),
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
                ('position','integer'),
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
                ('round','varchar(50)'),
                ('date','varchar(50)'),
                ('referee','varchar(50)'),
                ('opp','varchar(50)'),
                ('h_a','varchar(50)'),
                ('last5','varchar(50)'),
                ('result','varchar(50)'),
                ('point','integer'),
                ('gd','integer'),
                ('gf','integer'),
                ('ga','integer'),
                ('fhgf','integer'),
                ('fhga','integer'),
                ('shgf','integer'),
                ('shga','integer'),
                ('shot','integer'),
                ('shot_on_target','integer'),
                ('corner','integer'),
                ('foul','integer'),
                ('yellow','integer'),
                ('red','integer'),
                ('acc_point','integer'),
                ('acc_gd','integer'),
                ('acc_gf','integer'),
                ('position','integer')],
}

class FootballDB:
    
    global dic_table
    
    def __init__(self, host, user, port, pw, db_nm):
        self.host = host
        self.user = user
        self.port = port
        self.pw = pw
        self.db_nm = db_nm
        self.db = Postgres(self.host, self.user, self.port, self.pw) # postgres 객체 생성
        self.createDB() # database 없을 경우 생성
        self.createTables() # table 없을 경우 생성
        
    def createDB(self):
        """
        Database 생성
        """
        if not self.db.existDB(self.db_nm):
            self.db.createDB(self.db_nm)
    
    def dropDB(self):
        """
        Database 제거
        """
        self.db.dropDB(self.db_nm)
            
    def createTables(self):
        """
        등록되어 있는 여러 Table 생성
        """
        tbls = self.db.selectTables(self.db_nm)
        for tbl_nm, cols in dic_table.items():
            if tbl_nm not in tbls:
                self.db.createTable(self.db_nm, tbl_nm, cols)
                
    def createTable(self, tbl_nm, cols):
        """
        Table 생성

        Args:
            tbl_nm (str): Table 명
            cols (list): (column, column_type)으로 구성된 리스트
        """
        self.db.createTable(self.db_nm, tbl_nm, cols)
                
    def dropTable(self, tbl_nm):
        """
        Table 제거

        Args:
            tbl_nm (str): Table 명
        """
        self.db.dropTable(self.db_nm, tbl_nm)
                
    def insertTable(self, tbl_nm, data, on_colflict=False, conflict_col=None):
        """
        Table에 데이터 Insert

        Args:
            tbl_nm (str): 테이블 명
            data (DataFrame): 삽입할 데이터
            on_conflict (bool): 중복 방지 사용 여부
            conflict_col (str): 중복 여부 체크할 컬럼
        """
        cols = data.columns

        query1 = f"INSERT INTO {tbl_nm} ({','.join(col for col in cols)}) VALUES "
        query2 = f"{','.join([str(data) for data in data.itertuples(index=False, name=None)])}" if len(cols)>1 else f"{','.join([str(data).replace(',','') for data in data.itertuples(index=False, name=None)])}"
        if on_colflict:
            query2 += f" ON CONFLICT ({conflict_col}) DO NOTHING"
        query = query1 + query2
        query = query.replace('None', 'null')
        self.db.executeDB(self.db_nm, query)