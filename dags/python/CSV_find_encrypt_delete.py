import os
import sys
import argparse
import re
import pandas as pd

# блок после импорта os, sys нужен для корректной загрузки констант
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
from Diadoc_registry_ETL import DWNLD_FLDR, DAGS_FLDR


def find_diadoc_file(downloads_path):
    input_file_path = ''
    # Проверяем, существует ли папка
    if not os.path.exists(downloads_path):
        print("Папка 'Загрузки' не найдена.")
    else:
        # Перебираем все файлы в папке
        for file in os.listdir(downloads_path):
            # Проверяем, начинается ли имя файла с "Diadoc"
            if re.match(r'^Diadoc', file):
                # Сохраняем полный путь к файлу в переменной
                input_file_path = os.path.join(downloads_path, file)
                print(f"Файл 'Diadoc' найден: {input_file_path}")
                return input_file_path


def encrypt_diadoc_file(input_file_path, output_file_path, docs_type):
    # Преобразование кодировки
    if input_file_path:
        # Чтение исходного файла CSV
        df = pd.read_csv(input_file_path, encoding='windows-1251',
                         delimiter=';', low_memory=False)
        # Изменение заголовков столбцов
        columns_dict = {
            'ИНН': 'inn',
            'КПП': 'kpp',
            'Название организации': 'organization_name',
            'Имя файла': 'filename',
            'Номер документа': 'document_number',
            'Дата документа': 'document_date',
            'Всего': 'total',
            'Валюта': 'currency',
            'НДС': 'vat',
            'НДС 10%': 'vat_10',
            'НДС 18%': 'vat_18',
            'НДС 20%': 'vat_20',
            'Дата доставки': 'delivery_date',
            'Статус документа': 'document_status',
            'Дата тарификации': 'tarification_date',
            'Дата изменения статуса': 'status_change_date',
            'Подразделение': 'subdivision',
            'ФИО ответственного': 'responsible_person',
            'Комментарий': 'comment',
            'Ссылка': 'link',
            'Удален': 'deleted'
        }
        df.rename(columns=columns_dict, inplace=True)
        df['docs_type'] = docs_type
        print(df)
        print(df.info())
        print(list(columns_dict.values()))
        # Запись данных в новый файл CSV с новой кодировкой и разделителем
        df.to_csv(output_file_path, index=False, encoding='utf-8', sep=',')
        print(f"Файл 'Diadoc' преобразован в кодировку utf-8")
    else:
        print(f"Файл 'Diadoc' не найден")


if __name__ == "__main__":
    # передача параметров при запуске из терминала
    parser = argparse.ArgumentParser(
        description='Change encryption, column names of CSV file and put it in airflow load folder')
    parser.add_argument('--downloads_path', type=str, default=DWNLD_FLDR,
                        required=False,
                        help='Path to folder Download')
    parser.add_argument('--output_file_path', type=str,
                        default=f'{DAGS_FLDR}files/output.csv', required=False,
                        help='File path for output csv file')
    parser.add_argument('--docs_type', type=str, default='Outbox',
                        required=False,
                        help='Extra value for DataFrame assign')

    args = parser.parse_args()

    input_file_path = find_diadoc_file(args.downloads_path)
    encrypt_diadoc_file(input_file_path, args.output_file_path, args.docs_type)
    # Удаление файла из папки Загрузки
    os.remove(input_file_path)
    print("Файл успешно удален из папки Загрузки.")
