import sys
import time
import datetime
import requests
from bs4 import BeautifulSoup
import pandas
import pandas_gbq #pip install pandas-gbq
from google.oauth2 import service_account #pip install google-auth

def get_nextpage(url):
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    ret = soup.find("li", class_="Pager__list--next")
    if ret is None:
        print("最後のページです。")
        return None
    if ret.find("span", class_="Pager__link--disable") is None:
        next_url = ret.find("a").get("href")
        print(next_url)
        return next_url
    else:
        print("最後のページです。")
        return None

def get_items(url, df, columns):
    today = datetime.datetime.now()
    lastyear = today.year - 1
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    ret = soup.find_all("li", class_="Product")
    ret.pop(0) #最初の要素はヘッダーなので削除
    for item in ret:
        pid = item.find("a").get("href").replace("https://page.auctions.yahoo.co.jp/jp/auction/","")
        categorys = item.find_all("a", class_="Product__categoryLink")
        category_list = [x.text for x in categorys]
        category = " > ".join(category_list)
        title = item.find("a", class_="Product__titleLink").text
        end_price = item.find("span", class_="Product__priceValue").text.replace(",","").replace("円","")
        start_price = item.find("span", class_="u-fontSize14").text.replace(",","").replace("円","")
        bid_number = item.find("a", class_="Product__bid").text
        time = item.find("span", class_="Product__time").text
        #年の表示がないので年を先頭に付与（過去120日の表示なので、現在月が5月以前なら8月～12月は前年）
        if today.month <= 5:
            if 8 <= int(time[:2]) <= 12:
                time = str(lastyear) + "/" + time
            else:
                time = str(today.year) + "/" + time
        else:
            time = str(today.year) + "/" + time

        if item.find("span", class_="Product__icon--freeShipping") is None:
            freeshipping = 0
        else:
            freeshipping = 1
        print("{0}番目の商品({1}):{2}をDataFrameに追加します...".format(len(df),pid,title))
        print("カテゴリ:{0} 落札価格:{1} 開始価格:{2} 入札数:{3} 終了時間:{4} 送料無料の有無:{5}"\
              .format(category,end_price,start_price,bid_number,time,freeshipping))
        se = pandas.Series([pid , category, title, end_price, start_price, bid_number, time, freeshipping], columns)
        df = df.append(se, ignore_index=True)
    return df

def to_gbq(df):
    table_id = "my_dataset.yauc"
    project_id = "summer-flux-hoge123"
    credentials = service_account.Credentials.from_service_account_file("My Project 1111-hogehoge.json")
    pandas_gbq.to_gbq(df, table_id, project_id, if_exists='append', credentials=credentials)
    print("DataFrameをデータベースに書き込み完了しました。")

def main():
    columns = ["pid", "category", "title", "end_price", "start_price", "bid_number", "time", "freeshipping"]
    df = pandas.DataFrame(columns=columns)
    # 引数から取得 実行例：python yaucscraping.py args
    args = sys.argv
    query = args[1]
    url = "https://auctions.yahoo.co.jp/closedsearch/closedsearch?p={}&n=100".format(query)

    while True:
        print("商品情報を取得します...")
        df = get_items(url, df, columns)
        time.sleep(5)
        url = get_nextpage(url)
        time.sleep(5)
        if url is None:
            break
    df["time"] = pandas.to_datetime(df["time"])
    df = df.astype({"end_price": 'int', "start_price": 'int', "bid_number": 'int',"freeshipping": 'int'})
    return df

if __name__ == '__main__':
    df = main()
    to_gbq(df)
