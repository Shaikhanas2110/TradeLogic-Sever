import requests

access_token = "your_token"
headers = {"Accept": "application/json", "Authorization": "Bearer " + access_token}

# Test TCS directly
url = "https://api.upstox.com/v2/historical-candle/NSE_EQ|INE467B01029/day/2026-03-13/2026-03-13"
r = requests.get(url, headers=headers)
print(r.json())