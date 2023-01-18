import requests
import json
import pymongo
import pandas as pd
import datetime as dt

conn_str = f"mongodb://..."
mongo_client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
# session = requests.session()

vheaders = {
    "Accept": "application/x.seometrics.v3+json",
    "Accept-Language":"pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "none",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
}

def fn_readfilecsv():

    dinitial = 377000
    dfinal = 380000

    print(f"Reading input file {dinitial}:{dfinal}")
    
    filename = "all_domains.csv"
    input_domain = (
        pd.read_csv(
            f"{filename}",
            sep=",",
            usecols=[0],
            # nrows=20,
        )
        ["Domain"].str.replace("https://", "").str.replace("http://", "").str.strip()
    )
    return input_domain[dinitial:dfinal]

gdata = {}

def fn_get_info() -> dict:

    domains = fn_readfilecsv()
    domains = [website for website in domains]
    domains = str(domains).replace("'","#")
    domains = domains.replace("#",'"')

    domains_format = {"domains": domains, 
                      "country": "us", 
                      "version": "10.20", 
                      "api_key": ""}
    
    # teste_domain = '{"domains":["100thieves.com", "40boxes.com", "4patriots.com", "backtotheroots.com", "badbirdiegolf.com", "ballwash.com", "bantamtools.com", "bariatriceating.com"],"country":"us","version":"10.20","api_key":""}'

    domains_format = str(domains_format).replace("'", "#")
    domains_format = domains_format.replace("#", '"')

    if '"[' in domains_format:
        domains_format = domains_format.replace('"[', "[")

    if ']"' in domains_format:
        domains_format = domains_format.replace(']"', "]")

    url = "https://data.keywordseverywhere.com/service/get-domain-metrics"
    r = requests.post(url, headers=vheaders, data=domains_format)
    json_response = json.loads(r.content)
    # print(json_response)

    contador = 0

    print(f"Reading domains...")

    for info in json_response:
        gdata[f"d{contador + 1}"] = info
        # print(gdata)
        
        db_json = {
            "last_modified": dt.datetime.utcnow(),
            "input_domain": gdata[f"d{contador + 1}"]["domain"],
            "final_domain": gdata[f"d{contador + 1}"]["domain"],
            "monthly_traffic": gdata[f"d{contador + 1}"]["data"]["etv"],
            "monthly_traffic_format": gdata[f"d{contador + 1}"]["data"]["etv_format"],
            "total_keywords": gdata[f"d{contador + 1}"]["data"]["total_keywords"],
            "total_keywords_format": gdata[f"d{contador + 1}"]["data"]["total_keywords_format"],
        }
        contador += 1

        # Verify if domains already exist in database
        query_verifydomain = {"input_domain": db_json["input_domain"]}
        mongo = mongo_client["domains"]["keywordeverwhere"].find(query_verifydomain)
        mongo = [x for x in mongo]

        if len(mongo) == 0:
            mongo_client["domains"]["keywordeverwhere"].insert_one(db_json)
            print(db_json["input_domain"],"inserted.")
        
        elif len(mongo) == 1:
            print(db_json["input_domain"],"already exist in database")

        else:
            mongo_client["domains"]["keywordeverwhere"].delete_many(query_verifydomain)
            mongo_client["domains"]["keywordeverwhere"].insert_one(db_json)
            print(db_json["input_domain"], "deleted and inserted")
    
    return db_json

fn_get_info()
