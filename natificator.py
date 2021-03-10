import requests                   
from bs4 import BeautifulSoup    
from multiprocessing import Pool  
import sqlite3
import pymysql.cursors  
import time


def func_kinki(url):
    page = requests.get(url)
    page.encoding = 'windows-1251'
    root = BeautifulSoup(page.text, 'html.parser')
    try:
        domens = [el.text for el in root.find(attrs={'class': 'nodash'}).find_all('a')]
    except:
        domens = []
    pustoi = []
    if domens == []:
        return [' ']
    else:
        for x in domens:
            massiv = []
            iskl = ['vk.com','youtube.com','facebook.com','twitter.com','ok.ru/','instagram','google.com','odnoklassniki']
            for i in iskl:
                massiv.append(x.find(i) == -1)
            temp = massiv[0]
            for l in massiv[1:]:
                temp = temp and l
            if temp == True:
                pustoi.append(x)
        #print(pustoi)
        return pustoi


def get_html(url):
    r = requests.get(url)
    return r.text

def get_all_links(html, links):
    soup = BeautifulSoup(html, 'lxml')
    MASSIV = []
    first = []
    massiv = []
    for link in soup.find_all('a'):
        mould = link.get('href')
        massiv.append(mould)

    for x in massiv:
        link = str(x)
        if link.find('coinfo') != -1:
            links.append('https://cbr.ru%s'%(link))

    news = soup.findAll('td', {'class': ''})
    for i in range(len(news)):
        MASSIV.append(news[i].text)

    for i in range(int(len(MASSIV)/8)):
        obrabotka = [MASSIV[i*8+2],MASSIV[i*8+6],links[i]]
        #print(obrabotka)
        if obrabotka[1] == 'ОТЗ' or obrabotka[1] == 'АНН':
            first.append([obrabotka[0],links[i],'bad_domen'])
        else:
            first.append([obrabotka[0],links[i],'good_domen'])
    
    #print(links)
    return first


def create_table(connection):
    try:
        sql = "Create table natificator like final_table"
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()
    except:
        sql = "Truncate table natificator"
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()

def make_all(mould):
    url = mould[1]
    ident = mould[0]
    nazvanie = mould[2]
    data = func_kinki(url)
    #print(data)
    write_db(ident,data,nazvanie)

def write_db(ident,data,tablename):
    connection = pymysql.connect(host='################',
                             user='################',
                             password='################',                             
                             db='#####################',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

    sql = "Insert into natificator values ('%s','%s','%s')"
    with connection.cursor() as cursor:
        for l in data:
            #print(sql%(tablename,ident,l))
            cursor.execute(sql%(ident,l,tablename,))
            connection.commit()



def find_all(sql_zapros,connection):
    with connection.cursor() as cursor:
            mould1 = []
            cursor.execute(sql_zapros)
            for row in cursor:
                mould1.append(row)
    return mould1

def find_out(sql_zapros,connection):
    with connection.cursor() as cursor:
            mould1 = []
            cursor.execute(sql_zapros)
            for row in cursor:
                mould1.append(row['domen'])
    return mould1

def write_db2(ident,domen,list_domen,connection):
    sql = "Insert into final_table values ('%s','%s','%s')"
    with connection.cursor() as cursor:
        cursor.execute(sql%(ident,domen,list_domen))
        connection.commit()

def final_obrabotka(part,connection):
    a = find_out("SELECT distinct(domen) FROM final_table where list_domen = '%s'"%(part),connection)
    b = find_out("SELECT distinct(domen) FROM natificator where list_domen = '%s'"%(part),connection)
    d = find_all("SELECT * from natificator",connection)

    result1=list(set(b) - set(a))
    result2=list(set(a) - set(b))

    if result1 == [] and result2 == []:
        print ("Расхождения не обнаружены")
    elif result1 != [] and result2 == []:
        print ("Обнаружен недостаток данных в базе на локальной машине, происходит добавление")
        # Появились новые данные в списке на сайте, необходимо добавить данные на лок машину
        for x in result1:
            for y in d:
                if y['domen'] == x:
                    write_db2(y['ident'],y['domen'],part,connection)

    elif result1 == [] and result2 != []:
        print("Обнаружено удаление данных на сайте, происходит удаление данных на локальной машине")
        # Данные были удалены на сайте, необходимо удалить данные из базы данных на локальной машине
        for y in result2:
            sql = "call sql_delete ('%s')"
            with connection.cursor() as cursor:
                cursor.execute(sql%y)
                connection.commit()

def main():
    start_time = time.time()
    url = 'https://www.cbr.ru/banking_sector/credit/FullCoList/'
    links = []
    good = get_all_links(get_html(url), links)
    #print(good)
    connection = pymysql.connect(host='##############',
                             user='########',
                             password='###########',                             
                             db='###############',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    
    create_table(connection)

    with Pool(16) as p:           
       p.map(make_all, good)

    print("good domen")
    final_obrabotka('good_domen',connection)
    print("--------------------")
    print("bad domen")
    final_obrabotka('bad_domen',connection)

    print("--- %s seconds ---" % (time.time() - start_time))
     
if __name__ == '__main__':
    main()