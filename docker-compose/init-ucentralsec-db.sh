#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER $UCENTRALSEC_DB_USER WITH ENCRYPTED PASSWORD '$UCENTRALSEC_DB_PASSWORD';
    CREATE DATABASE $UCENTRALSEC_DB;
    GRANT ALL PRIVILEGES ON DATABASE $UCENTRALSEC_DB TO $UCENTRALSEC_DB_USER;
EOSQL