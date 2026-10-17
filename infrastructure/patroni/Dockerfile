# Patroni поверх официального образа PostgreSQL 17.
# Собственная сборка вместо ghcr-образов: прозрачный patroni.yml, нативный arm64,
# зависимости только с Docker Hub и PyPI.
FROM postgres:17.2

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && pip3 install --no-cache-dir --break-system-packages "patroni[etcd3]==4.1.5" "psycopg2-binary==2.9.10" \
    && apt-get purge -y python3-pip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# patroni сам инициализирует PGDATA, поэтому обходим entrypoint официального образа.
# Официальный entrypoint создаёт data-каталог с правами 0700 в рантайме; без него
# том наследует 1777 от /var/lib/postgresql, и postgres отказывается стартовать.
ENV PGDATA=/var/lib/postgresql/data
RUN install -d -m 700 -o postgres -g postgres /var/lib/postgresql/data

COPY patroni.yml /etc/patroni.yml
COPY bootstrap-with-appdb.sh /usr/local/bin/bootstrap-with-appdb.sh
RUN chmod 755 /usr/local/bin/bootstrap-with-appdb.sh

USER postgres
EXPOSE 5432 8008

CMD ["patroni", "/etc/patroni.yml"]
