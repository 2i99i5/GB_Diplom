import os
import sys
import argparse
import time
from selenium import webdriver  # класс управления браузером
from selenium.webdriver.chrome.options import Options  # Настройки
from selenium.webdriver.common.by import By  # селекторы
from selenium.webdriver.support.ui import WebDriverWait  # класс для ожидания
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from datetime import date, timedelta

# блок после импорта os, sys нужен для корректной загрузки secret.secret
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from secret.secret import DIADOC_LOGIN, DIADOC_PASS
from Diadoc_registry_ETL import DWNLD_FLDR, LOAD_TIME, INTERVAL, YEAR

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
LINK = "https://diadoc.kontur.ru/"


def download_registry(docs_type='Outbox', load_time=LOAD_TIME,
                      date_range_mode=INTERVAL, year=YEAR):
    link = LINK
    email = DIADOC_LOGIN
    user_agent = USER_AGENT
    password = DIADOC_PASS

    chrome_option = Options()
    chrome_option.add_argument('--headless')
    chrome_option.add_argument(f'{user_agent=}')
    driver = webdriver.Chrome(options=chrome_option)
    params = {'behavior': 'allow', 'downloadPath': DWNLD_FLDR}
    driver.execute_cdp_cmd('Page.setDownloadBehavior', params)
    print(f'headless initialized {params=} ')
    print(f'Путь = {os.getcwd()}')
    try:
        driver.get(link)
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(2)

        # авторизация
        # переход на закладку входа по паролю
        bookmark_el = driver.find_element(By.XPATH,
                                          '//*[contains(text(), "Пароль")]')
        bookmark_el.click()
        wait = WebDriverWait(driver, 1)
        # ввод логина
        email_input = wait.until(
            EC.presence_of_element_located((By.ID, "email")))
        email_input.send_keys(email)
        time.sleep(2)
        # ввод пароля
        password_input = wait.until(
            EC.presence_of_element_located((By.ID, "password")))
        password_input.send_keys(password)
        time.sleep(1)
        password_input.send_keys(Keys.ENTER)
        time.sleep(2)
        print('autorization success')
        time.sleep(1)

        # Входящие или Исходящие
        label = driver.find_element(By.XPATH,
                                    f'//li/a[@tid="{docs_type}"]').click()
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(1)

        # расширенный поиск
        advanced_search = driver.find_element(By.XPATH,
                                              '//div[@tid = "AdvancedSearchCollapsibleTrigger"]/a')
        advanced_search.click()
        time.sleep(1)

        # выбор интервала даты
        select_date_interval = driver.find_element(By.XPATH,
                                                   '//div[@tid="DateRangeMode"]/span/span/button')
        select_date_interval.click()
        options = driver.find_element(By.XPATH, '//div[@tid="DateRange"]')
        date_range = options.find_element(By.XPATH,
                                          f'//*[contains(text(), "{date_range_mode}")]')
        date_range.click()
        time.sleep(1)

        if date_range_mode == 'Интервал':
            # ввод даты начала интервала
            today = date.today()
            yesterday = today - timedelta(days=1)
            hidden_input_value = yesterday.strftime('%d%m%Y')
            print(hidden_input_value)
            date_from_day = driver.find_element(By.XPATH,
                                                '//div[@tid="DateRangeFromDay"]')
            time.sleep(3)
            date_from_day.click()
            # Use JavaScript to set the value of the hidden input
            script = f"document.querySelector('span:nth-child(2)>div>div>label>span>input[type = hidden]').value = '{hidden_input_value}';"
            driver.execute_script(script)
            time.sleep(5)

            # ввод даты конца интервала
            hidden_input_value = today.strftime('%d%m%Y')
            print(hidden_input_value)
            date_to_day = driver.find_element(By.XPATH,
                                              '//div[@tid="DateRangeToDay"]')
            time.sleep(3)
            date_to_day.click()
            # Use JavaScript to set the value of the hidden input
            script = f"document.querySelector('span:nth-child(4)>div>div>label>span>input[type=hidden]').value = '{hidden_input_value}';"
            driver.execute_script(script)
            time.sleep(5)
        elif date_range_mode == 'Год':
            # ввод года
            year_input = driver.find_element(By.XPATH,
                                             '//div[@tid = "DateRangeYear"]/div/label/span/input')
            year_input.send_keys(str(year))
            print(f'Введён {year} год в поле')
            time.sleep(5)

        # нажатие кнопки поиска
        find_button = driver.find_element(By.XPATH,
                                          '//div[@tid = "AdvancedSearchFooter"]/div/div/span[1]/span/button')
        find_button.click()
        time.sleep(10)
        # после поиска появляется кнопка скачивания реестра csv
        # нажимаем её
        download_registry_btn = driver.find_element(By.XPATH,
                                                    '//span[@locstr = "Skachat_reestr_dokumentov"]')
        download_registry_btn.click()
        print(f'Реестр документов формируется')
        # реестр формируется в зависимости от количества документов в интервале
        time.sleep(load_time)
        # файл *.csv сохраняется средствами браузера в папку по умолчанию


    except Exception as er:
        print(f'Произошла ошибка: {er}')
    finally:
        driver.close()
        driver.quit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Selenium. Download CSV registry from Diadoc: Outbox, Inbox')
    parser.add_argument('--docs_type', type=str, default='Inbox',
                        required=False,
                        help='Type of documents: Outbox Inbox')
    parser.add_argument('--load_time', type=int, default=LOAD_TIME,
                        required=False,
                        help='Wait time for download')
    parser.add_argument('--date_range_mode', default=INTERVAL, type=str,
                        required=False,
                        help='Mode: Год, Интервал')
    parser.add_argument('--year', type=int, default=YEAR, required=False,
                        help='Year for download')
    args = parser.parse_args()

    download_registry(args.docs_type, args.load_time, args.date_range_mode,
                      args.year)
