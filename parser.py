import requests                   
from bs4 import BeautifulSoup    
from multiprocessing import Pool  
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
        try:
            obrabotka = [MASSIV[i*8+2],MASSIV[i*8+6],links[i]]
            if obrabotka[1] == 'ОТЗ' or obrabotka[1] == 'АНН':
                first.append([obrabotka[0],links[i],'bad_domen'])
            else:
                first.append([obrabotka[0],links[i],'good_domen'])
        except:
            continue
    
    return first

def make_all(mould):
    url = mould[1]
    ident = mould[0]
    nazvanie = mould[2]
    data = func_kinki(url)
    write_db(ident,data,nazvanie)


def write_db(ident,data,tablename):
    
    connection = pymysql.connect(host='###################',
                             user='#################',
                             password='################',                             
                             db='######################',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    sql = "Insert into final_table values ('%s','%s','%s')"
    with connection.cursor() as cursor:
        for l in data:
            #print(sql%(tablename,ident,l))
            cursor.execute(sql%(ident,l,tablename,))
            connection.commit()


def main():
    #start_time = time.time()
    url = 'https://www.cbr.ru/banking_sector/credit/FullCoList/'
    
    links = []
    good = get_all_links(get_html(url), links)

    #with Pool(30) as p:           
     #  p.map(make_all, good)
    #print("--- %s seconds ---" % (time.time() - start_time))
     
if __name__ == '__main__':
    main()