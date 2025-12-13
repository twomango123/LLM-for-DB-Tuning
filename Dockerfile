# Dockerfile for ch-benchmark + MySQL ODBC
FROM ubuntu:22.04

# 避免交互提示
ENV DEBIAN_FRONTEND=noninteractive
# odbc驱动
ENV LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libodbc
# 安装基本开发工具和依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    g++ \
    make \
    python3 \
    python3-pip \
    python3-dev \
    cmake \
    libodbc1 \
    unixodbc \
    unixodbc-dev \
    odbcinst \
    odbcinst1debian2 \
    wget \
    curl \
    odbc-mariadb \
    git \
    vim \
    nano \
    gdb \
    sudo \
    lsb-release \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# 安装 MySQL 服务端、客户端和必要依赖
RUN apt-get update && \
    apt-get install -y mysql-server mysql-client && \
    rm -rf /var/lib/apt/lists/*

# 设置 MySQL root 密码
RUN service mysql start && \
    mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '123!@#200'; FLUSH PRIVILEGES;" && \
    mysql -uroot -p'123!@#200' -e "CREATE DATABASE IF NOT EXISTS tpcch;" 

# 配置ODBC 驱动 数据源
RUN echo "[MySQL ODBC 8.0 Unicode Driver]"        > /etc/odbcinst.ini && \
    echo "Description = MySQL ODBC 8.0 Unicode Driver" >> /etc/odbcinst.ini && \
    echo "Driver      = /usr/lib/x86_64-linux-gnu/odbc/libmaodbc.so" >> /etc/odbcinst.ini && \
    echo "UsageCount  = 1"     >> /etc/odbcinst.ini

RUN echo "[mysql-bench]" > /etc/odbc.ini && \
echo "Driver   = MySQL ODBC 8.0 Unicode Driver" >> /etc/odbc.ini && \
echo "Server   = localhost" >> /etc/odbc.ini && \
echo "Port     = 3306" >> /etc/odbc.ini && \
echo "Database = tpcch" >> /etc/odbc.ini && \
echo "User     = root" >> /etc/odbc.ini && \
echo "Password = 123!@#200" >> /etc/odbc.ini

RUN apt-get update && apt-get install -y valgrind


# 创建工作目录
WORKDIR /LLM-for-DB-Tuning

# 将工作目录复制到容器中
COPY ./LLM-for-DB-Tuning /LLM-for-DB-Tuning


RUN service mysql start 
# 暴露端口
EXPOSE 3306

# source venv/bin/activate

# docker rm -f 73f7dfe2b656
# docker rmi ch-schema 
# pip freeze > requirements.txt

# cd ~/SchemaTuning
# docker build -t schematuning-docker .


# docker run -d --name schematuning-docker \
# -p 3306:3306 -p 8080:8080 \
# schematuning-docker

# docker build -t chbenchmark:latest .

# apt-get install -y odbc-mariadb  下载这个
# isql -U root -P '123!@#200' -S localhost 

# mysql -u root -p tpcch
# describe order_primary_key;
# drop table order_primary_key;
# SHOW VARIABLES LIKE 'secure_file_priv';

# ./chBenchmark -csv -wh 1 -pa /var/lib/mysql-files

# cd /var/lib/mysql-files
# for f in *.tbl; do
#     mv "$f" "$(echo $f | tr 'A-Z' 'a-z')"
# done

# nano /etc/mysql/mysql.conf.d/mysqld.cnf
# secure-file-priv=/tmp/chcsv, /tmp/output/
# ./chBenchmark -run -dsn mysql_chbench -usr root -pwd '123!@#200' -a 5 -t 10 -wd 60 -td 300 -pa /var/lib/mysql-files -op /var/lib/mysql-files


# 进入容器
# docker exec -it schematuning-docker bash

# 每次记得更新 复制到根目录
# pip freeze > /LLM-for-DB-Tuning/requirements.txt 

# 停止
# docker stop schematuning-docker

# docker run -d \
#   --name schematuning \
#   --cpus="2" \
#   --memory="4g" \
#   --network none \       # 可选：禁止外网访问，保证完全封闭
#   -p 3306:3306 \
#   -p 8080:8080 \
#   schematuning-docker


# docker build -t chtest
# docker run -it chtest bash

# test 

# export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libodbc

# service mysql start
# # mysql -u root -p
# CREATE DATABASE tpcch;

# 配置数据源
# nano /etc/odbc.ini 
# [mysql-bench]
# Driver = MySQL ODBC 8.0 Unicode Driver
# Server = localhost
# Port = 3306
# Database = tpcch
# User = root
# Password = 123!@#200

# 配置驱动
# nano /etc/odbcinst.ini 
# [MySQL ODBC 8.0 Unicode Driver]
# Description = MySQL ODBC 8.0 Unicode Driver
# Driver = /usr/lib/x86_64-linux-gnu/odbc/libmaodbc.so

# docker build -t chtest
# docker run -it chtest bash

# service mysql start
# isql -U root -P '123!@#200' -S localhost 
# cd /LLM-for-DB-Tuning/ch-benchmark
# make
# ./chBenchmark -csv -wh 1 -pa /var/lib/mysql-files

# cd /var/lib/mysql-files
# for f in *.tbl; do
#     mv "$f" "$(echo $f | tr 'A-Z' 'a-z')"
# done

# a是AP线程，t是TP线程数，a=1,t=0测试AP latency,反之测试TP latency，测试latency需要test duation设置长一点
# cd /LLM-for-DB-Tuning/ch-benchmark
# ./chBenchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 0 -t 1 -wd 10 -td 100 -pa /var/lib/mysql-files -op /var/lib/mysql-files
# 测吞吐的一般参数
# ./chBenchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 5 -t 10 -wd 60 -td 300 -pa /var/lib/mysql-files -op /var/lib/mysql-files