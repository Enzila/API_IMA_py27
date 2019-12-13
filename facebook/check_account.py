from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import csv

def writecsv(namefile,type,header=None):
    with open(namefile, 'w') as f:
        w = csv.writer(f)
        if header is not None:
            w.writerow(header)
        for row in type:
            w.writerow(row)

if __name__ == '__main__':

    options = Options()
    options.add_argument('--headless')

    read = open('log_facebook_account.txt', 'r')
    list = []
    for row in read:
        list.append(row.replace('\n',''))

    active_account = []
    not_found_account = []

    driver = webdriver.Firefox(options=options)
    for account in list:
        driver.get("https://fb.com/{}".format(account))
        print account
        try:
            a = driver.find_element_by_css_selector('#u_0_c > div')
            print (a.text)
            if a.text == 'Anda harus masuk untuk melanjutkan.':
                not_found_account.append(account)
                print ("{0} not found".format(account))
            else:
                active_account.append(account)
                print ("{0} active account".format(account))
        except:
            active_account.append(account)
            print ("{0} active account".format(account))
            continue

    print active_account
    writecsv('FB_available_account.csv', active_account, ['user_id'])
    print not_found_account
    writecsv('FB_not_found_account.csv', not_found_account, ['user_id'])