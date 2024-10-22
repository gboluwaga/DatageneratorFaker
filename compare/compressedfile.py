import requests
import json

# Get data using an API
response = requests.get('https://www.mmobomb.com/api1/games')
data = json.loads(response.text)

categories = set()

for item in data:
    categories.add(item["title"])

print(categories)

