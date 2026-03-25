FROM postgres:16-alpine

# Instalar Bucardo y dependencias
RUN apk add --no-cache \
    perl \
    perl-dbi \
    perl-dbix-safe \
    perl-dev \
    make \
    gcc \
    musl-dev \
    postgresql-dev \
    curl

# Instalar módulos Perl necesarios para Bucardo
RUN curl -L https://cpanmin.us | perl - \
    DBD::Pg \
    boolean \
    Sys::Syslog

# Descargar e instalar Bucardo
RUN curl -L https://bucardo.org/downloads/Bucardo-5.6.0.tar.gz | tar xz && \
    cd Bucardo-5.6.0 && \
    perl Makefile.PL && \
    make && \
    make install && \
    cd .. && rm -rf Bucardo-5.6.0

COPY init-bucardo.sh /docker-entrypoint-initdb.d/init-bucardo.sh
RUN chmod +x /docker-entrypoint-initdb.d/init-bucardo.sh
