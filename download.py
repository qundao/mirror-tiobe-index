import logging
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

URL = "https://www.tiobe.com/tiobe-index/"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"

MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "SEPT": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def parse_row(tr):
    out = []
    for i, td in enumerate(tr.find_all("td")):
        if i == 2 and td.img:
            value = td.img["src"].split("/")[-1].split(".")[0]
        elif i == 3:
            continue
        else:
            value = td.text.strip()
        out.append(value)
    return out


def download(save_dir="."):
    logging.info(f"Request {URL}")
    headers = {"User-Agent": USER_AGENT}

    try:
        res = requests.get(URL, headers=headers, timeout=10)
        res.raise_for_status()
    except Exception as e:
        logging.error(f"Exception = {e}")
        return

    soup = BeautifulSoup(res.text, "html.parser")
    a = soup.article

    logging.info("Parse top20")
    table = a.find("table", id="top20")
    cols1 = [th.text.strip() for tr in table.thead.find_all("tr") for th in tr.find_all("th")]
    data1 = [parse_row(tr) for tr in table.tbody.find_all("tr")]
    key_col = "Programming Language"
    cols1[2] = "Position#"

    logging.info("Parse top50")
    table = a.find("table", id="otherPL")
    cols2 = [th.text.strip() for tr in table.thead.find_all("tr") for th in tr.find_all("th")]
    data2 = [[td.text.strip() for td in tr.find_all("td")] for tr in table.tbody.find_all("tr")]
    cols2[0] = cols1[0]

    df1 = pd.DataFrame(data1, columns=cols1)
    df2 = pd.DataFrame(data2, columns=cols2)
    df = pd.concat([df1, df2])

    logging.info("Parse top100")
    if a.ul and a.ul.li:
        more = a.ul.li.text
        if more:
            top_100 = more.strip().split(", ")
            data3 = [[str(i), name] for i, name in enumerate(top_100, len(df) + 1)]
            df3 = pd.DataFrame(data3, columns=cols2[:2])
            df = pd.concat([df, df3])

    month, year = cols1[0].split()
    month = MONTHS[month.upper()]
    year = int(year)
    save_name = f"{year}/{year}-{month:02d}.tsv"

    logging.info(f"Save to {save_name}")
    save_file = Path(save_dir, save_name)
    if not save_file.parent.exists():
        save_file.parent.mkdir(parents=True)
    out_cols = [key_col] + [c for c in df.columns if c != key_col]
    df[out_cols].to_csv(save_file, sep="\t", index=False, na_rep="")


if __name__ == "__main__":
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)
    download()
