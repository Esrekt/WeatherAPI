from json.decoder import NegInf

import pandas as pd
import sqlalchemy
import requests
import time
from datetime import date
from datetime import datetime
import pyodbc
import matplotlib.pyplot as plt

# SQL Server bağlantı bilgileri
server = 'DESKTOP-VF1FSBT\\SQLEXPRESS'  # SQL Server adresi
database = 'hava_durumu'  # Veritabanı adı

# Bağlantı dizesi
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                      f'SERVER={server};'
                      f'DATABASE={database};'
                      'Trusted_Connection=yes;')

sehirler=[" Adana","Adıyaman","Afyonkarahisar","Ağrı","Aksaray","Amasya","Ankara","Antalya","Ardahan", "Artvin", "Aydın","Balıkesir","Bartın", "Batman","Bayburt","Bilecik","Bingöl","Bitlis","Bolu","Burdur","Bursa","Çanakkale","Çankırı","Çorum","Denizli","Diyarbakır","Düzce","Edirne","Elazığ","Erzincan","Erzurum","Eskişehir","Gaziantep","Giresun","Gümüşhane","Hakkâri","Hatay","Iğdır","Isparta","İstanbul","İzmir","Kahramanmaraş","Karabük","Karaman","Kars","Kastamonu","Kayseri","Kilis","Kırıkkale","Kırklareli","Kırşehir","Kocaeli","Konya","Kütahya","Malatya","Manisa","Mardin","Mersin","Muğla","Muş","Nevşehir","Niğde","Ordu","Osmaniye","Rize","Sakarya","Samsun","Şanlıurfa","Siirt","Sinop","Sivas","Şırnak","Tekirdağ","Tokat","Trabzon","Tunceli","Uşak","Van","Yalova","Yozgat","Zonguldak"]
cursor = conn.cursor()
for sehir in sehirler:
    print(sehir)
    URL = f"https://api.openweathermap.org/data/2.5/weather?q={sehir}&appid=0579d17c54ac1c305ac37e953cbd4e79&lang=tr&units=metric"
    getWeather = requests.get(URL)
    getWeatherData = getWeather.json()
    sehir=getWeatherData["id"]
    sıcaklık= getWeatherData["main"]["temp"]
    basınc=getWeatherData["main"]["humidity"]
    den_sev=getWeatherData["main"]["sea_level"]
    nem= getWeatherData["main"]["pressure"]
    #simdi ben bu sehirlerin ıd lerine bakıp dicem ki sehir varsa update le yoksa ekle
    update_insert_query = '''
    IF EXISTS (SELECT * FROM havadurumu WHERE sehir= ?)
    BEGIN
         UPDATE havadurumu
         SET sıcaklık = ?, basınc = ?, den_sev = ?, nem = ?
         WHERE sehir = ?
    END
    ELSE
    BEGIN
   
         INSERT INTO havadurumu (sehir,sıcaklık, basınc, den_sev, nem)
         VALUES (?, ?, ?, ?, ?)
    END
    '''

    cursor.execute(update_insert_query,
                   (sehir, sıcaklık, basınc, den_sev, nem, sehir,
                    sehir, sıcaklık, basınc, den_sev, nem))
    #cursor veritabanına sql sorguları calıstırmak icin kullanılan bir arayüzdür. veritabanına gönderilen komutları
    #temsil eder ve sonucları almak icin gerekli yöntemleri sağlar
    #değişiklikleri kaydeder
    cursor.commit()
    #mode a ile dosya sonuna veri ekler
    #tablo.to_csv("cikti7.csv",mode="a",index=False)
    #datam=pd.read_csv("cikti7.csv")

    time.sleep(1)
#ve kapatır
cursor.close()
print("işlem tamamlandı")





