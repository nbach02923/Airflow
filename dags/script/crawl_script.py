import csv
import requests
from datetime import datetime
from bs4 import BeautifulSoup
def crawling():
    base_url = "https://raovat321.com/bat-dong-san"
    header = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 OPR/97.0.0.0"
    }
    records = []
    for n in range(1, 5):
        url = base_url + "?page=" + str(n)
        response = requests.get(url, headers=header)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            linkTags = soup.find_all(class_="w-24 md:w-32 flex-shrink-0")
            linksStrip = [linkTag["href"] for linkTag in linkTags]
            for linkAppend in linksStrip:
                link = "https://raovat321.com" + linkAppend
                detailResponse = requests.get(link)
                detailSoup = BeautifulSoup(detailResponse.content, "html.parser")
                idDiv = detailSoup.find(class_="ml-auto flex items-center space-x-6")
                strong_text = idDiv.find("strong").decompose()
                id = int(idDiv.find("span").text)
                title = detailSoup.find(class_="text-2xl font-bold").text
                date = (
                    detailSoup.find(class_="mt-2 text-category")
                    .text.strip(" ")
                    .strip("\n")
                    .strip("   ")
                )
                priceText = detailSoup.find(
                    class_="text-red-600 text-lg font-bold"
                ).text
                p = priceText.split(" ")
                price = 0
                if len(p) > 1 and p[1] == "tỷ":
                    price = int(float(p[0].replace(",", ".")) * 1000000000)
                elif len(p) > 1 and p[1] == "triệu":
                    price = int(float(p[0].replace(",", ".")) * 1000000)
                elif len(p) > 1 and p[1] == "ngàn":
                    price = int(float(p[0].replace(",", ".")) * 1000)
                else:
                    price = 0
                div = detailSoup.find_all("div", class_="mt-2")
                div = detailSoup.find_all("div", class_="mt-2")
                addressString = div[3]
                address = addressString.find(class_="text-category").text
                postOwnerPlaceholder = detailSoup.find("div", class_="w-full text-sm")
                linkToOwner = postOwnerPlaceholder.find_all("a")
                uplOwner = linkToOwner[0].text
                record = {}
                record.update(
                    {
                        "id": id,
                        "title": title,
                        "date": date,
                        "price": price,
                        "address": address,
                        "owner": uplOwner,
                    }
                )
                records.append(record)
    filename_csv = "bat_dong_san_" + datetime.today().strftime("%d_%m_%Y") + ".csv"
    with open("./dags/data/real_estate" + filename_csv, "w+", encoding="utf-8", newline="") as c:
        writer = csv.DictWriter(
            c,
            fieldnames=["id", "title", "date", "price", "address", "owner"],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(records)
        c.close()