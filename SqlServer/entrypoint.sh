#!/bin/bash

# Script usuario e criação database
/usr/config/configure-db.sh &

# Iniciando SQL Server
/opt/mssql/bin/sqlservr