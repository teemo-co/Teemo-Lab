import pandas as pd
from tqdm import tqdm
from json import loads

dfs = pd.read_csv('raw_matches_1000_2024_0710.csv', chunksize=1000)

data= []
for df in dfs:
    for match in tqdm(df.match_data):
        info = loads(match)["info"]
        for part in info["participants"]:
            data.append(
                {
                    "summonerName": part["summonerName"],
                    "summonerId": part["summonerId"],
                    "enemyMissingPings": part.get("enemyMissingPings", 0),  # 기본값 0
                }
            )

# print(len(data))
# for d in data[:20]:
#     print(d)

# 데이터를 pandas DataFrame으로 변환
pings_df = pd.DataFrame(data)

# 유저별 게임 횟수 계산
counts = pings_df.groupby("summonerId").size().reset_index(name="matchCount")

# 유저별 enemyMissingPings 합산
pings_df = pings_df.groupby("summonerId", as_index=False).agg({
    "summonerName": "first",  # 가장 먼저 등장한 summonerName을 선택
    "enemyMissingPings": "sum"  # enemyMissingPings 값 합산
})

# 합산된 데이터에 등장 횟수 추가
pings_df = pd.merge(pings_df, counts, on="summonerId")

# # pandas 출력 옵션 설정: 열과 행 생략 없이 표시
pd.set_option("display.max_columns", None)  # 모든 열 출력
pd.set_option("display.max_rows", None)    # 모든 행 출력 (필요시 조정)

# enemyMissingPings 순으로 정렬
pings_df = pings_df.sort_values(by="enemyMissingPings", ascending=False)

# 상위 20줄 출력
print(pings_df.head(20))

# 결과를 CSV 파일로 저장
pings_df.to_csv('output.csv', index=False)
