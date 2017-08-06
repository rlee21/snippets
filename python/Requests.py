import requests
from bs4 import BeautifulSoup

url = "http://www.rotoworld.com/headlines/nba/0/Basketball-headlines"
data = requests.get(url)
# print(data.content)
soup = BeautifulSoup(data.content)
# print(soup.prettify())
# print(soup.find_all("a"))

# for link in soup.find_all("a"):
    # print(link.get("href"))
    # print(link.text)
    # print(link.text, link.get("href"))
for item in soup.find_all("div", {"class":"impact"}):
    info = item.text
    print(info)
    # with open("outfile.txt","w") as outfile:
    #     outfile.writelines(info)

