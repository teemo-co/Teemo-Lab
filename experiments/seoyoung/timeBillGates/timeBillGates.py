import pandas as pd
from tqdm import tqdm
from json import loads

dfs = pd.read_csv('raw_matches_1000_2024_0710.csv', chunksize=1000)

data = []
for df in dfs:
    for match in tqdm(df.match_data):
        info = loads(match)["info"]
        for part in info["participants"]:
            data.append(
                [
                    info["gameDuration"],
                    part["deaths"],
                    part["summonerName"],
                    part["summonerId"]
                ]
            )
# print(len(data))
# for d in data[:20]:
#     print(d)

# 데이터를 pandas DataFrame으로 변환
processed_df = pd.DataFrame(data, columns=["gameDuration", "deaths", "summonerName", "summonerId"])
result = (
    processed_df.groupby("summonerId", as_index=False)  # summonerId별로 그룹화
    .agg(
        summonerName=("summonerName", "first"),        # 첫 번째 summonerName 가져오기
        totalGameDuration=("gameDuration", "sum")     # gameDuration 합산
    )
    .sort_values(by="totalGameDuration", ascending=False)  # 게임시간 합산 결과로 정렬
)
# print(result)
result.to_csv('output.csv', index=False)

# summonerId별 등장 횟수와 summonerName 추가
result2 = (
    processed_df.groupby("summonerId", as_index=False)
    .agg(
        summonerName=("summonerName", "first"),  # summonerName의 대표값 (첫 번째 값)
        count=("summonerId", "count")           # summonerId의 등장 횟수
    )
    .sort_values(by="count", ascending=False)   # 등장 횟수 기준 내림차순 정렬
)
# print(result2)
result2.to_csv('output2.csv', index=False)

