#!/usr/bin/python3.8
import apache_log_parser
import ftplib
from apache_mysql import MySQLi
from config import *
from random import randrange
import datetime
import os

"""
Создаём объект класса БД.
Создаем .txt файл.
Подключаемся к FTP.
Сохраняем текущую дату в переменную - часы по UTC поясу.
"""
db = MySQLi(host, user, password, database_home)
local_file = f'{randrange(1000)}.txt'
ftp_connect = ftplib.FTP(host_config, ftp_user_config, ftp_password_config)
datetime = datetime.datetime.utcnow().date()
file_delete = local_file
"""
Проверяем все файлы с FTP сервера на предыдущий парсинг.
Если файл отличается, добавляем его в БД и записываем в локальный файл.
"""
with open(local_file, 'wb') as local_file:
    for file in ftp_connect.nlst():
        if "access.log" in file:
            file_info = ftp_connect.size(file)
            file_name = file
            file_names = db.fetch("SELECT file_name, file_size, datetime FROM file_info WHERE "
                                  "file_name = %s AND file_size = %s "
                                  "LIMIT 1",
                                  file_name,
                                  file_info
                                  )

            if not file_names['rows']:
                print(f"{file_name} - не было файлов лога в БД")
                ftp_connect.retrbinary('retr ' + file, local_file.write)
                db.commit("insert into file_info (file_name, file_size, datetime) VALUES (%s, %s, %s)",
                          file_name,
                          file_info,
                          datetime
                          )
            else:
                date_sql = file_names['rows'][-1][-1]
                print("Повторная проверка")
                if datetime != date_sql.date():
                    print(f"{file_name} - не было файлов лога в БД после повторной проверки")
                    print("Заносим в БД")
                    ftp_connect.retrbinary('retr ' + file, local_file.write)
                    db.commit("insert into file_info (file_name, file_size, datetime) VALUES (%s, %s, %s)",
                              file_name,
                              file_info,
                              datetime
                              )
ftp_connect.close()

"""
Разбираем логи апача из локального файла.
Проверяем на предмет дублей-записей в БД.
Дубли в БД не записываем.
"""
f = open(file_delete, 'r')
line_parser = apache_log_parser.make_parser("%v %h %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" %l")
for line_row in f:
    log_line_data = line_parser(line_row)
    server_name = log_line_data['server_name']
    ip = log_line_data['remote_host']
    code = log_line_data['status']
    url = log_line_data['request_url']
    ua = log_line_data['request_header_user_agent__browser__family']
    time = log_line_data['time_received']
    check_string_in_db = db.fetch("select server_name FROM apache_logs2 WHERE server_name= %s AND ip = %s AND "
                                  "status_code = %s AND url = %s AND ua = %s AND  time = %s",
                                  server_name, ip, code, url, ua, time)
    if not check_string_in_db['rows']:
        db.commit(
            "INSERT INTO apache_logs2 (server_name, ip, status_code, url, ua, version, time) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            log_line_data['server_name'], log_line_data['remote_host'],
            log_line_data['status'], log_line_data['request_url'],
            log_line_data['request_header_user_agent__browser__family'],
            log_line_data['request_header_user_agent__browser__version_string'],
            log_line_data['time_received']
        )
f.close()
os.remove(file_delete)
#