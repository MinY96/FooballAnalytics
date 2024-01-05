import pandas as pd
from datetime import datetime
from itertools import accumulate

def home_team_stat(home_rst):
    """
    선택한 팀의 홈 성적 추출

    Args:
        home_rst (DataFrame): 홈 경기 매치 결과

    Returns:
        Tuple: 승,무,패,골득실,득점,실점,경고,퇴장
    """
    wdl = home_rst['FTR']
    w, d, l = len(wdl[wdl=='H']), len(wdl[wdl=='D']), len(wdl[wdl=='A'])
    gf = int(home_rst['FTHG'].sum())
    ga = int(home_rst['FTAG'].sum())
    y = int(home_rst['HY'].sum())
    r = int(home_rst['HR'].sum())
    return w,d,l,gf,ga,y,r

def away_team_stat(away_rst):
    """
    선택한 팀의 원정 성적 추출

    Args:
        away_rst (DataTable): 원정 결기 매치 결과

    Returns:
        Tuple: 승,무,패,골득실,득점,실점,경고,퇴장
    """
    wdl = away_rst['FTR']
    w, d, l = len(wdl[wdl=='A']), len(wdl[wdl=='D']), len(wdl[wdl=='H'])
    ga = int(away_rst['FTHG'].sum())
    gf = int(away_rst['FTAG'].sum())
    y = int(away_rst['AY'].sum())
    r = int(away_rst['AR'].sum())
    return w,d,l,gf,ga,y,r

def season_position(league, season, match_data):
    """
    선택한 시즌의 테이블

    Args:
        league (str): 리그 정보
        season (str): 시즌 정보
        match_data (DataTable): 시즌 매치 결과

    Returns:
        DataTable: 시즌 순위표
    """
    ls_table = []
    teams = list(match_data['HomeTeam'].unique())
    for team in teams:
        h_w,h_d,h_l,h_gf,h_ga,h_y,h_r = home_team_stat(match_data[match_data['HomeTeam']==team])
        a_w,a_d,a_l,a_gf,a_ga,a_y,a_r = away_team_stat(match_data[match_data['AwayTeam']==team])
        
        w = h_w + a_w
        d = h_d + a_d
        l = h_l + a_l
        p = 3*w + d
        gf = h_gf + a_gf
        ga = h_ga + a_ga
        gd = gf - ga
        y = h_y + a_y
        r = h_r + a_r
        ls_table.append((team, p, w, d, l, gd, gf, ga, y, r))

    col_table = ['team', 'point', 'won', 'drawn', 'lost', 'gd', 'gf', 'ga', 'yellow', 'red']
    table = pd.DataFrame(ls_table, columns=col_table)
    # 승점 > 골득실 > 득점 > 승자승 > 원정 다득점 > 플레이오프
    table.sort_values(['point', 'gd', 'gf'], ascending=[False, False, False], inplace=True)
    # table.insert(0, 'position', [i+1 for i in range(table.shape[0])])
    table['position'] = [i+1 for i in range(table.shape[0])]
    table.reset_index(drop=True, inplace=True)
    table['league'] = league
    table['season'] = season
    return table[['league', 'season', 'position','team', 'point', 'won', 'drawn', 'lost', 'gd', 'gf', 'ga', 'yellow', 'red']]

def season(dt_season_position):
    """
    선택한 시즌의 팀 정보

    Args:
        dt_season_position (DataFrame): tbl_season_position
    """
    dt_season = dt_season_position[['league', 'season', 'team']].copy()
    dt_season['manager'] = None
    dt_season['captain'] = None
    dt_season.sort_values('team', inplace=True)
    return dt_season
    

def season_match_info(league, season, match_data):
    """
    선택한 시즌의 경기 결과

    Args:
        league (str): 리그 정보
        season (str): 시즌 정보
        match_data (DataTable): 시즌 매치 결과
        
    Returns:
        DataTable: 시즌 경기 상세 결과
    """
    teams = list(match_data['HomeTeam'].unique())
    ls_rst = []
    for team in teams:
        h_rst = match_data[match_data['HomeTeam']==team][['Date', 'FTR', 'FTHG', 'FTAG', 'AwayTeam', 'HTHG', 'HTAG', 'HS', 'HST', 'HF', 'HY', 'HR', 'HC', 'Referee']]
        h_rst.rename(columns={'Date':'date', 'FTHG':'gf', 'FTAG':'ga', 'AwayTeam':'opp', 'HTHG':'fhgf', 'HTAG':'fhga', 'HC':'corner',
                            'HS':'shot', 'HST':'shot_on_target', 'HF':'foul', 'HY':'yellow', 'HR':'red', 'Referee':'referee'}, inplace=True)
        h_rst = h_rst.astype({'gf':'int', 'ga':'int', 'fhgf':'int', 'fhga':'int', 'shot':'int', 'shot_on_target':'int', 'foul':'int', 'yellow':'int', 'red':'int', 'corner':'int'})
        h_rst['point'] = h_rst['FTR'].apply(lambda x: 3 if x=='H' else (1 if x=='D' else 0))
        h_rst['gd'] = h_rst['gf'] - h_rst['ga']
        h_rst['shgf'] = h_rst['gf'] - h_rst['fhgf']
        h_rst['shga'] = h_rst['ga'] - h_rst['fhga']
        h_rst['h_a'] = 'H'
        
        a_rst = match_data[match_data['AwayTeam']==team][['Date', 'FTR', 'FTHG', 'FTAG', 'HomeTeam', 'HTHG', 'HTAG', 'AS', 'AST', 'AF', 'AY', 'AR', 'AC', 'Referee']]
        a_rst.rename(columns={'Date':'date', 'FTHG':'ga', 'FTAG':'gf', 'HomeTeam':'opp', 'HTHG':'fhga', 'HTAG':'fhgf', 'AC':'corner',
                            'AS':'shot', 'AST':'shot_on_target', 'AF':'foul', 'AY':'yellow', 'AR':'red', 'Referee':'referee'}, inplace=True)
        a_rst = a_rst.astype({'gf':'int', 'ga':'int', 'fhgf':'int', 'fhga':'int', 'shot':'int', 'shot_on_target':'int', 'foul':'int', 'yellow':'int', 'red':'int', 'corner':'int'})
        a_rst['point'] = a_rst['FTR'].apply(lambda x: 3 if x=='A' else (1 if x=='D' else 0))
        a_rst['gd'] = a_rst['gf'] - a_rst['ga']
        a_rst['shgf'] = a_rst['gf'] - a_rst['fhgf']
        a_rst['shga'] = a_rst['ga'] - a_rst['fhga']
        a_rst['h_a'] = 'A'
        
        rst = pd.concat([h_rst, a_rst])    
        rst['league'] = league
        rst['season'] = season
        rst['team'] = team
        try:
            rst['date'] = rst['date'].apply(lambda x: datetime.strptime(x, '%d/%m/%y'))
        except:
            rst['date'] = rst['date'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y'))
        rst.sort_values('date', inplace=True)
        rst['acc_point'] = list(accumulate(rst['point']))
        rst['acc_gd'] = list(accumulate(rst['gd']))
        rst['acc_gf'] = list(accumulate(rst['gf']))
        rst['round'] = list(range(1, len(rst)+1))
        rst['result'] = rst['point'].apply(lambda p: 'W' if p==3 else ('D' if p==1 else 'L'))
        results = list(rst['result'])
        last5 = [''.join(results[i-5:i]) if i>5 else ''.join(results[:i]) for i in range(len(results))]
        rst['last5'] = last5
        rst.reset_index(drop=True, inplace=True)
        
        rst = rst[['league', 'season', 'team', 'round', 'date', 'referee', 'opp', 'h_a', 'last5', 'result', 'point', 
                'gd', 'gf', 'ga', 'fhgf', 'fhga', 'shgf', 'shga','shot', 'shot_on_target', 'corner', 'foul', 
                'yellow', 'red', 'acc_point', 'acc_gd', 'acc_gf']]
        ls_rst.append(rst)
    
    match_info = pd.concat(ls_rst, axis=0)
    match_info.reset_index(drop=True, inplace=True)

    position_by_round = [(id,pos+1) 
                        for round in range(1, 39) 
                        for pos, id in enumerate(match_info[match_info['round']==round].sort_values(['acc_point', 'acc_gd', 'acc_gf'], ascending=[False, False, False]).index)]
    position_by_round = sorted(position_by_round, key=lambda x:(x[0]))
    position_by_round = [pos for r,pos in position_by_round]

    match_info['position'] = position_by_round
    match_info['date'] = match_info['date'].dt.strftime('%Y-%m-%d')
    
    return match_info