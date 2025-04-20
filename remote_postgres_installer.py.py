#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import time
import argparse
import paramiko
import socket
from paramiko.ssh_exception import NoValidConnectionsError, AuthenticationException


class RemotePostgresInstaller:
    def __init__(self, hosts, ssh_key_path=None):
        """
        Инициализация установщика PostgreSQL
        :param hosts: список хостов (IP-адреса или имена)
        :param ssh_key_path: путь к SSH-ключу (если None, будет использован ~/.ssh/id_rsa)
        """
        self.hosts = hosts
        self.ssh_key_path = ssh_key_path or os.path.expanduser('~/.ssh/id_rsa')
        self.ssh_port = 22
        self.ssh_user = 'root'

    def get_ssh_client(self, host):
        """
        Создание и настройка SSH-клиента
        :param host: целевой хост
        :return: настроенный SSH-клиент
        """
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                hostname=host,
                port=self.ssh_port,
                username=self.ssh_user,
                key_filename=self.ssh_key_path,
                timeout=10
            )
            print(f"[+] Подключен к {host}")
            return client
        except (NoValidConnectionsError, AuthenticationException, socket.error) as e:
            print(f"[-] Ошибка подключения к {host}: {str(e)}")
            return None

    def execute_command(self, client, command):
        """
        Выполнение команды на удаленном хосте
        :param client: SSH-клиент
        :param command: выполняемая команда
        :return: вывод команды (stdout, stderr)
        """
        try:
            stdin, stdout, stderr = client.exec_command(command, get_pty=True)
            stdout_data = stdout.read().decode('utf-8')
            stderr_data = stderr.read().decode('utf-8')
            return stdout_data, stderr_data
        except Exception as e:
            return "", f"Ошибка выполнения команды: {str(e)}"

    def get_host_load(self, client):
        """
        Получение информации о загрузке сервера
        :param client: SSH-клиент
        :return: средняя загрузка системы за 1 минуту
        """
        try:
            stdout, stderr = self.execute_command(client, "cat /proc/loadavg")
            if stdout:
                # Средняя загрузка за 1 мин (первое значение)
                load_avg = float(stdout.split()[0])
                return load_avg
            else:
                print(f"[-] Не удалось получить информацию о загрузке: {stderr}")
                return float('inf')  # Возвращаем "бесконечность" при ошибке
        except Exception as e:
            print(f"[-] Ошибка при получении информации о загрузке: {str(e)}")
            return float('inf')

    def detect_os(self, client):
        """
        Определение операционной системы на удаленном хосте
        :param client: SSH-клиент
        :return: тип ОС ('debian', 'centos' или 'unknown')
        """
        # Пробуем определить ОС через os-release
        stdout, _ = self.execute_command(client, "cat /etc/os-release")
        
        if "debian" in stdout.lower():
            return "debian"
        elif "centos" in stdout.lower() or "almalinux" in stdout.lower() or "rhel" in stdout.lower():
            return "centos"
        
        # Пробуем другие методы определения
        stdout, _ = self.execute_command(client, "which apt-get")
        if stdout and not stdout.startswith("which:"):
            return "debian"
        
        stdout, _ = self.execute_command(client, "which yum")
        if stdout and not stdout.startswith("which:"):
            return "centos"
        
        return "unknown"

    def install_postgresql(self, client, os_type):
        """
        Установка PostgreSQL на удаленный хост
        :param client: SSH-клиент
        :param os_type: тип ОС ('debian' или 'centos')
        :return: успешность установки
        """
        print(f"[*] Устанавливаем PostgreSQL на {os_type} сервер...")
        
        if os_type == "debian":
            # Установка PostgreSQL на Debian
            commands = [
                "apt-get update",
                "apt-get install -y postgresql postgresql-contrib",
                "systemctl status postgresql",
            ]
            
            for cmd in commands:
                print(f"[*] Выполняем: {cmd}")
                stdout, stderr = self.execute_command(client, cmd)
                if "failed" in stderr.lower() or "error" in stderr.lower():
                    print(f"[-] Ошибка: {stderr}")
                    return False
            
            return True
            
        elif os_type == "centos":
            # Установка PostgreSQL на CentOS/AlmaLinux
            commands = [
                "dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm",
                "dnf -qy module disable postgresql",
                "dnf install -y postgresql14-server",
                "systemctl enable postgresql-14",
                "/usr/pgsql-14/bin/postgresql-14-setup initdb",
                "systemctl start postgresql-14",
                "systemctl status postgresql-14",
            ]
            
            for cmd in commands:
                print(f"[*] Выполняем: {cmd}")
                stdout, stderr = self.execute_command(client, cmd)
                if "failed" in stderr.lower() or "error" in stderr.lower():
                    print(f"[-] Ошибка: {stderr}")
                    return False
                
            return True
        
        else:
            print(f"[-] Неподдерживаемая ОС: {os_type}")
            return False

    def configure_postgresql(self, client, os_type, other_host):
        """
        Настройка PostgreSQL для приема внешних соединений
        :param client: SSH-клиент
        :param os_type: тип ОС ('debian' или 'centos')
        :param other_host: IP-адрес второго сервера
        :return: успешность настройки
        """
        print("[*] Настраиваем PostgreSQL для приема внешних соединений...")
        
        # Определяем пути к конфигурационным файлам в зависимости от ОС
        if os_type == "debian":
            pg_hba_path = "/etc/postgresql/14/main/pg_hba.conf"
            postgresql_conf_path = "/etc/postgresql/14/main/postgresql.conf"
            restart_cmd = "systemctl restart postgresql"
        elif os_type == "centos":
            pg_hba_path = "/var/lib/pgsql/14/data/pg_hba.conf"
            postgresql_conf_path = "/var/lib/pgsql/14/data/postgresql.conf"
            restart_cmd = "systemctl restart postgresql-14"
        else:
            print(f"[-] Неподдерживаемая ОС для настройки PostgreSQL: {os_type}")
            return False
        
        # Настройка postgresql.conf для приема внешних соединений
        cmd = f"sed -i \"s/#listen_addresses = 'localhost'/listen_addresses = '*'/g\" {postgresql_conf_path}"
        self.execute_command(client, cmd)
        
        # Backup pg_hba.conf
        self.execute_command(client, f"cp {pg_hba_path} {pg_hba_path}.bak")
        
        # Добавление строки для доступа с другого сервера
        append_cmd = f"""cat <<EOF >> {pg_hba_path}
# Allow user "student" from the other server
host    all             student         {other_host}/32            md5
# Allow connections from all addresses
host    all             all             0.0.0.0/0                 md5
EOF"""
        self.execute_command(client, append_cmd)
        
        # Создание пользователя student
        create_user_cmd = """
su - postgres -c "psql -c \\"CREATE USER student WITH PASSWORD 'StrongPassword123!';\\"" 
su - postgres -c "psql -c \\"ALTER USER student CREATEDB;\\"" 
"""
        stdout, stderr = self.execute_command(client, create_user_cmd)
        if "ERROR" in stderr:
            print(f"[-] Ошибка создания пользователя: {stderr}")
            return False
        
        # Перезапуск PostgreSQL для применения изменений
        self.execute_command(client, restart_cmd)
        
        print("[+] PostgreSQL успешно настроен для приема внешних соединений!")
        return True

    def test_postgresql(self, client, os_type):
        """
        Проверка работы PostgreSQL выполнением тестового запроса
        :param client: SSH-клиент
        :param os_type: тип ОС ('debian' или 'centos')
        :return: результат проверки
        """
        print("[*] Проверяем работу PostgreSQL...")
        
        test_cmd = 'su - postgres -c "psql -c \\"SELECT 1 as test_connection;\\"" '
        stdout, stderr = self.execute_command(client, test_cmd)
        
        if "test_connection" in stdout and "1" in stdout:
            print("[+] PostgreSQL успешно отвечает на SQL-запросы!")
            return True
        else:
            print(f"[-] PostgreSQL не отвечает на запросы: {stderr}")
            return False

    def run(self):
        """
        Основной метод для выполнения всей логики установки
        :return: успешность выполнения
        """
        print(f"[*] Начинаем процесс установки PostgreSQL на один из серверов: {', '.join(self.hosts)}")
        
        # Проверяем подключение и получаем загрузку серверов
        clients = {}
        loads = {}
        
        for host in self.hosts:
            client = self.get_ssh_client(host)
            if client:
                clients[host] = client
                load = self.get_host_load(client)
                loads[host] = load
                print(f"[+] Загрузка сервера {host}: {load}")
        
        if not clients:
            print("[-] Не удалось подключиться ни к одному серверу")
            return False
        
        # Выбираем наименее загруженный сервер
        target_host = min(loads, key=loads.get)
        other_host = [h for h in self.hosts if h != target_host][0]  # Второй хост
        
        print(f"[+] Выбран целевой сервер для установки PostgreSQL: {target_host} (загрузка: {loads[target_host]})")
        
        # Определяем ОС целевого сервера
        os_type = self.detect_os(clients[target_host])
        print(f"[+] Определена ОС на целевом сервере: {os_type}")
        
        if os_type == "unknown":
            print("[-] Не удалось определить ОС на целевом сервере")
            return False
        
        # Устанавливаем PostgreSQL
        if not self.install_postgresql(clients[target_host], os_type):
            print("[-] Не удалось установить PostgreSQL")
            return False
        
        # Настраиваем PostgreSQL
        if not self.configure_postgresql(clients[target_host], os_type, other_host):
            print("[-] Не удалось настроить PostgreSQL")
            return False
        
        # Проверяем работу PostgreSQL
        if not self.test_postgresql(clients[target_host], os_type):
            print("[-] PostgreSQL не прошел проверку работоспособности")
            return False
        
        print(f"\n[+] Установка PostgreSQL успешно завершена на сервере {target_host}!")
        print(f"[+] Пользователь 'student' может подключаться с IP-адреса {other_host}")
        
        # Закрываем соединения
        for client in clients.values():
            client.close()
        
        return True


def main():
    """
    Главная функция для запуска приложения
    """
    parser = argparse.ArgumentParser(description='Установка PostgreSQL на удаленный сервер')
    parser.add_argument('hosts', help='IP-адреса или имена серверов (через запятую)')
    parser.add_argument('--key', help='Путь к SSH-ключу (по умолчанию ~/.ssh/id_rsa)')
    
    args = parser.parse_args()
    
    # Парсим список хостов, разделенных запятой
    hosts = [host.strip() for host in args.hosts.split(',')]
    
    if len(hosts) != 2:
        print("[-] Необходимо указать ровно два сервера, разделенных запятой")
        return 1
    
    installer = RemotePostgresInstaller(hosts, args.key)
    success = installer.run()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
