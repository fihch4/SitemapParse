f = open('text.txt', 'r')
import apache_log_parser
import ftplib
from apache_mysql import MySQLi
from config import *


host = host_config
ftp_user = ftp_user_config
ftp_password = ftp_password_config

ftp = ftplib.FTP(host, ftp_user, ftp_password)

list_files = ftp.nlst()
ftp.close()
print(list_files)
local_file = 'apache_logs.txt'
with open(local_file, 'wb') as local_file:
    for file in list_files:
        if "access.log" in file:
            ftp = ftplib.FTP(host, ftp_user, ftp_password)
            print(file)
            ftp_file = file
            ftp.retrbinary('retr ' + ftp_file, local_file.write)
            ftp.close()

line_parser = apache_log_parser.make_parser("%v %h %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" %l")

for line_row in f:
    log_line_data = line_parser(line_row)
    server_name = log_line_data['server_name']
    ip = log_line_data['remote_host']
    code = log_line_data['status']
    url = log_line_data['request_url']
    ua = log_line_data['request_header_user_agent__browser__family']
    version = log_line_data['request_header_user_agent__browser__version_string']
    time = log_line_data['time_received']
    print(f"{server_name} - {ip} - {code} - {url} - {ua} - {version} - {time}")
    db = MySQLi(host, user, password, database_home)
    db.commit(
        "INSERT INTO apache_logs2 (server_name, ip, status_code, url, ua, version, time) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        server_name, ip, code, url, ua, version, time)