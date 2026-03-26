FROM postgres:16

RUN apt-get update && apt-get install -y \
    bucardo \
    postgresql-plperl-16 \
    && rm -rf /var/lib/apt/lists/*

COPY init-bucardo.sh /docker-entrypoint-initdb.d/init-bucardo.sh
RUN chmod +x /docker-entrypoint-initdb.d/init-bucardo.sh