# Архитектура и конфигурация: NiFi → Managed Service for Apache Kafka (Yandex Cloud)

> Документ описывает текущее состояние решения по отправке данных из Apache NiFi,
> развёрнутого в Docker на ноутбуке, в кластер Yandex Managed Service for Apache Kafka.
> Все параметры кластера, топиков и пользователей приведены по фактическому состоянию
> ресурсов, полученному через API на 2026-11-19.

## Содержание

1. [Общая схема](#1-общая-схема)
2. [Параметры кластера](#2-параметры-кластера)
3. [Аппаратные ресурсы](#3-аппаратные-ресурсы)
4. [Топики](#4-топики)
5. [Пользователи и права](#5-пользователи-и-права)
6. [Сетевой доступ](#6-сетевой-доступ)
7. [Реестр схем](#7-реестр-схем)
8. [Скрипты конфигурации](#8-скрипты-конфигурации)
9. [Настройки NiFi](#9-настройки-nifi)
10. [Хронология выполненных шагов](#10-хронология-выполненных-шагов)
11. [Ключевой вывод по форматам схем](#11-ключевой-вывод-по-форматам-схем)
12. [Открытые вопросы](#12-открытые-вопросы)

---

## 1. Общая схема


┌────────────────────────────────────────────┐
│ Ноутбук (macOS) · Docker                   │
│                                            │
│   ┌──────────────────────────────────┐     │
│   │ Apache NiFi                      │     │
│   │  PublishKafkaRecord_2_0          │     │
│   │  AvroRecordSetWriter             │     │
│   └──────────────────────────────────┘     │
│                                            │
│   kafkactl (CLI)                           │
└──────────────────────┬─────────────────────┘
                       │
                       │  HTTPS 443     → Schema Registry
                       │  SASL_SSL 9091 → Kafka
                       │  (публичный доступ)
                       ▼
┌────────────────────────────────────────────┐
│ Yandex Cloud                               │
│ каталог b1garhid9ikqi6ek09j9               │
│                                            │
│   ┌──────────────────────────────────┐     │
│   │ Managed Service for Kafka        │     │
│   │ кластер kafka777                 │     │
│   │ c9q6v0lk049fbhvqe8dh             │     │
│   │ Kafka 4.2.1 · HA                 │     │
│   │ 3 брокера в 3 зонах AZ           │     │
│   │                                  │     │
│   │  rc1a-…  (ru-central1-a)         │     │
│   │  rc1d-…  (ru-central1-d)         │     │
│   │  rc1f-…  (ru-central1-f)         │     │
│   │                                  │     │
│   │ топики: MyTopic-1, -2, -3        │     │
│   │ пользователь: kafka-user         │     │
│   │ Schema Registry (Karapace)       │     │
│   │  субъект MyTopic-3-value: AVRO   │     │
│   └──────────────────────────────────┘     │
│                                            │
│   VPC enp6c84e3v0hhq6otv81                 │
│   SG  enpq2v71d2llnuv5tovs                 │
└────────────────────────────────────────────┘


Поток данных: NiFi читает файл → разбирает записи через Record Reader →
сериализует через `AvroRecordSetWriter` со ссылкой на схему в реестре →
`PublishKafkaRecord_2_0` отправляет в топик `MyTopic-3` по SASL_SSL на порт 9091.

---

## 2. Параметры кластера

| Параметр | Значение |
|---|---|
| Имя / ID | `kafka777` / `c9q6v0lk049fbhvqe8dh` |
| Каталог | `b1garhid9ikqi6ek09j9` |
| Облако | `b1gmkmkjok9sp2oatv98` |
| Версия Kafka | 4.2.1 (major 4.2) |
| Окружение | `PRODUCTION` |
| Статус / health | `RUNNING` / `ALIVE` |
| Создан | 2026-11-19 07:49 UTC |
| Отказоустойчивость | `is_ha: true` |
| Зоны доступности | `ru-central1-a`, `ru-central1-d`, `ru-central1-f` |
| Публичный доступ | включён (`assign_public_ip: true` на всех хостах) |
| Реестр схем | включён (`schema_registry: true`) |
| Kafka REST API | включён (`rest_api_config.enabled: true`) |
| Kafka UI | включён — `https://ui-c9q6v0lk049fbhvqe8dh.kafka.yandexcloud.net` |
| Управляемые топики | да (`unmanaged_topics: false`) |
| Защита от удаления | включена (`deletion_protection: true`) |
| Окно обслуживания | суббота, 02:00–03:00 UTC, допускается временная недоступность |
| Плановые работы | «Maintain Kafka cluster DNS records», отложены до 2026-11-28 02:04 UTC |
| Фактор репликации по умолчанию | 3 |
| Сеть / группа безопасности | `enp6c84e3v0hhq6otv81` / `enpq2v71d2llnuv5tovs` |

---

## 3. Аппаратные ресурсы

### Брокеры

Три хоста с идентичной конфигурацией:

| Параметр | Значение |
|---|---|
| Роль | `KAFKA` |
| Пресет | `s3-c2-m8` — 2 vCPU, 8 ГБ RAM, платформа Intel Ice Lake |
| Диск | 270 ГиБ `network-ssd` на хост |
| Суммарно | 810 ГиБ |
| Health | `ALIVE` на всех трёх |

### Размещение

| FQDN | Зона | Подсеть |
|---|---|---|
| `rc1a-6782bojupms8aio6.mdb.yandexcloud.net` | `ru-central1-a` | `e9brh8k5t51bfsree9ns` |
| `rc1d-ob5dnc3pcqcm6jq4.mdb.yandexcloud.net` | `ru-central1-d` | `fl8u6f5c4sc1sm0202cm` |
| `rc1f-4bv4lq02sat1509r.mdb.yandexcloud.net` | `ru-central1-f` | `b1lmo0bl5gfb31ohk1u3` |

### Клиентская сторона

Ноутбук macOS, Docker с контейнером NiFi, утилита `kafkactl`,
Cloud Shell для выполнения API-запросов.

---

## 4. Топики

Все три топика идентичны по конфигурации:

| Топик | Разделы | RF | cleanup.policy | retention.ms | segment.bytes |
|---|---|---|---|---|---|
| `MyTopic-1` | 3 | 3 | `delete` | 604800000 (7 сут.) | 1073741824 (1 ГиБ) |
| `MyTopic-2` | 3 | 3 | `delete` | 604800000 (7 сут.) | 1073741824 (1 ГиБ) |
| `MyTopic-3` | 3 | 3 | `delete` | 604800000 (7 сут.) | 1073741824 (1 ГиБ) |

Не заданы (действуют значения по умолчанию): `retention.bytes`,
`max.message.bytes`, `min.insync.replicas`, `compression.type`,
`delete.retention.ms`, `file.delete.delay.ms`, `flush.messages`, `flush.ms`,
`preallocate`.

При RF=3 и трёх брокерах каждая партиция хранится на всех трёх хостах —
данные топика занимают тройной объём на диске.

---

## 5. Пользователи и права

`kafka-user` — единственный пользователь кластера.

| Топик | Роли |
|---|---|
| `MyTopic-1` | `ACCESS_ROLE_CONSUMER`, `ACCESS_ROLE_PRODUCER`, `ACCESS_ROLE_TOPIC_ADMIN` |
| `MyTopic-2` | `ACCESS_ROLE_CONSUMER`, `ACCESS_ROLE_PRODUCER`, `ACCESS_ROLE_TOPIC_ADMIN` |
| `MyTopic-3` | `ACCESS_ROLE_CONSUMER`, `ACCESS_ROLE_PRODUCER`, `ACCESS_ROLE_TOPIC_ADMIN` |

Ограничений `allow_hosts` нет. По документации такие права на топик позволяют
управлять субъектами `<топик>-key`, `<топик>-value` и `<топик>` в реестре схем —
этого достаточно для регистрации схемы `MyTopic-3-value`.

---

## 6. Сетевой доступ

Группа безопасности `enpq2v71d2llnuv5tovs`
(`default-sg-enp6c84e3v0hhq6otv81`), статус `ACTIVE`, помечена как default для сети:

| Направление | Протокол | Порты | Источник / назначение |
|---|---|---|---|
| INGRESS | ANY (-1) | все | `0.0.0.0/0` |
| EGRESS | ANY (-1) | все | `0.0.0.0/0` |

Порты сервиса:

| Порт | Протокол подключения | Назначение |
|---|---|---|
| 9091 | `SASL_SSL` | Kafka с шифрованием — единственный вариант из интернета |
| 9092 | `SASL_PLAINTEXT` | Kafka без шифрования — только внутри VPC |
| 443 | HTTPS | Managed Schema Registry, Kafka REST API |

---

## 7. Реестр схем

| Параметр | Значение |
|---|---|
| Реализация | Karapace, API совместим с Confluent Schema Registry |
| Адрес | `https://<FQDN брокера>:443` |
| Авторизация | логин/пароль пользователя Kafka (`kafka-user`) |
| TLS | тот же клиентский сертификат, что для брокеров |
| Хранение | служебный топик `__schema_registry` |
| Поддерживаемые форматы | Avro, JSON Schema, Protobuf |

### Зарегистрированные субъекты

| Субъект | Формат | Назначение | Статус |
|---|---|---|---|
| `MyTopic-3` | JSON Schema (draft-07, `title: User`) | первая попытка, для `JsonRecordSetWriter` | не используется |
| `MyTopic-3-value` | **AVRO** | рабочая схема для `AvroRecordSetWriter` | **используется** |

Схема `MyTopic-3-value` описывает объект `User` с полями:

| Поле | Тип | Описание |
|---|---|---|
| `id` | integer | уникальный идентификатор пользователя |
| `name` | string | имя пользователя |
| `age` | integer | возраст |
| `email` | string | электронная почта |

Все четыре поля обязательны.

---

## 8. Скрипты конфигурации

### 8.1. Корневой сертификат и truststore

```bash
mkdir -p ~/nifi-certs && cd ~/nifi-certs
curl -o YandexInternalRootCA.crt https://storage.yandexcloud.net/cloud-certs/CA.pem
keytool -importcert -alias YandexCA -file YandexInternalRootCA.crt \
  -keystore truststore.jks -storepass changeit -noprompt
```

Для Windows:
```bash
mkdir $HOME\.kafka
curl.exe --ssl-no-revoke -o $HOME\.kafka\YandexInternalRootCA.crt `
  https://storage.yandexcloud.net/cloud-certs/CA.pem
keytool.exe -importcert -alias YandexCA `
  --file $HOME\.kafka\YandexInternalRootCA.crt `
  --keystore $HOME\.kafka\ssl --storepass <пароль> --noprompt
```

### 8.2. Конфиг kafkactl

~/.config/kafkactl/config.yml:
```text
contexts:
  kafka777:
    brokers:
      - rc1a-6782bojupms8aio6.mdb.yandexcloud.net:9091
      - rc1d-ob5dnc3pcqcm6jq4.mdb.yandexcloud.net:9091
      - rc1f-4bv4lq02sat1509r.mdb.yandexcloud.net:9091
    tls:
      enabled: true
      ca: /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt
    sasl:
      enabled: true
      username: kafka-user
      password: "<пароль>"
      mechanism: scram-sha512
current-context: kafka777
```

Критичные детали: username (не user), mechanism: scram-sha512
(без дефиса перед 512).

### 8.3. Создание и настройка топиков
```bash
yc managed-kafka topic create MyTopic-1 --cluster-name kafka777 \
  --partitions 3 --replication-factor 3

yc managed-kafka topic update MyTopic-1 --cluster-name kafka777 \
  --cleanup-policy delete \
  --retention-ms 604800000 \
  --segment-bytes 1073741824
```

### 8.4. Регистрация Avro-схемы (рабочий вариант)
```bash
cat > /tmp/user.avsc <<'EOF'
{
  "type": "record",
  "name": "User",
  "namespace": "ru.example.kafka",
  "fields": [
    { "name": "id",    "type": "int",    "doc": "Уникальный идентификатор пользователя" },
    { "name": "name",  "type": "string", "doc": "Имя пользователя" },
    { "name": "age",   "type": "int",    "doc": "Возраст" },
    { "name": "email", "type": "string", "doc": "Электронная почта" }
  ]
}
EOF

jq -n --rawfile s /tmp/user.avsc '{schemaType:"AVRO", schema:$s}' \
| curl --request POST \
  --url 'https://rc1a-6782bojupms8aio6.mdb.yandexcloud.net:443/subjects/MyTopic-3-value/versions' \
  --user kafka-user:'<пароль>' \
  --cacert ~/YandexInternalRootCA.crt \
  --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data @-
```
Успешный ответ — идентификатор схемы, например {"id":2}.

### 8.5. Проверка регистрации
```bash
#### список субъектов
curl --request GET \
  --url 'https://rc1a-6782bojupms8aio6.mdb.yandexcloud.net:443/subjects' \
  --user kafka-user:'<пароль>' --cacert ~/YandexInternalRootCA.crt
  
# последняя версия схемы субъекта
curl --request GET \
  --url 'https://rc1a-6782bojupms8aio6.mdb.yandexcloud.net:443/subjects/MyTopic-3-value/versions/latest' \
  --user kafka-user:'<пароль>' --cacert ~/YandexInternalRootCA.crt
```
### 8.6. Docker с NiFi
```text
volumes:
  - ~/nifi-certs:/opt/nifi/nifi-current/certs:ro
```

Каталог с truststore монтируется внутрь контейнера только для чтения.
## 9. Настройки NiFi

### 9.1. StandardSSLContextService

| Свойство | Значение |
|---|---|
| Truststore Filename | `/opt/nifi/nifi-current/certs/truststore.jks` |
| Truststore Password | `changeit` |
| Truststore Type | `JKS` |
| TLS Protocol | `TLSv1.2` |

### 9.2. ConfluentSchemaRegistry

| Свойство | Значение |
|---|---|
| Schema Registry URL | `https://rc1a-6782bojupms8aio6.mdb.yandexcloud.net:443` |
| Basic Authentication Username | `kafka-user` |
| Basic Authentication Password | пароль пользователя |
| SSL Context Service | ссылка на `StandardSSLContextService` |

Оба контроллер-сервиса должны быть в состоянии **Enabled** до запуска процессора.

### 9.3. AvroRecordSetWriter

| Свойство | Значение |
|---|---|
| Schema Write Strategy | `Confluent Schema Registry Reference` |
| Schema Access Strategy | `Use 'Schema Name' Property` |
| Schema Name | `MyTopic-3-value` |
| Schema Registry | ссылка на `ConfluentSchemaRegistry` |

### 9.4. PublishKafkaRecord_2_0

| Свойство | Значение |
|---|---|
| Kafka Brokers | три FQDN на порту 9091 |
| Security Protocol | `SASL_SSL` |
| SASL Mechanism | `SCRAM-SHA-512` |
| Username / Password | `kafka-user` / пароль |
| SSL Context Service | `StandardSSLContextService` |
| Topic Name | `MyTopic-3` |
| Record Reader | под формат входного файла (`JsonTreeReader`, `CSVReader` и т.п.) |
| Record Writer | `AvroRecordSetWriter` |
| Delivery Guarantee | `Guarantee Delivery of Messages` (`acks=all`) |
| Use Transactions | `false` |

### 9.5. Проверка результата

```bash
kafkactl consume MyTopic-3 --from-beginning
```

При стратегии Confluent Schema Registry Reference writer не встраивает схему
в сообщение, а пишет идентификатор схемы по формату Confluent
(magic byte + 4-байтовый id). Поэтому в начале сообщений будет бинарный префикс —
это ожидаемое поведение, а не ошибка. Читать такие сообщения нужно клиентом
с поддержкой Schema Registry.

## 10. Хронология выполненных шагов

1. **Создан кластер** `kafka777` (`c9q2qdqmatvf9g3ii9b1`), Kafka 4.1.2,
   3 брокера в двух зонах, диски `network-hdd` 273 ГиБ, `is_ha: false`.
2. **Созданы топики** `MyTopic-1..3` — 3 раздела, RF=3.
3. **Настроены параметры топиков**: `cleanup.policy=delete`,
   `retention.ms=604800000`, `segment.bytes=1073741824`.
4. **Диагностика подключения с macOS**: TCP до 9091 проходил, но данные
   рвались (`connection reset by peer` на partition 1).
5. **Диагностика с Windows и мобильной сети**: выявлен отказ хоста
   `rc1a-ittlifonvbauo1om` (`84.201.129.95`) — ICMP проходит, TCP на
   9091/9092/443 в таймаут из двух независимых сетей.
6. **Кластер пересоздан** — `c9q6v0lk049fbhvqe8dh`, Kafka 4.2.1, три зоны,
   `network-ssd` 270 ГиБ, `is_ha: true`, защита от удаления.
7. **Топики и пользователь пересозданы** с теми же параметрами.
8. **Исправлен конфиг kafkactl**: `mechanism: scram-sha512`,
   `username` вместо `user`.
9. **Развёрнут NiFi в Docker** для чтения файла и отправки в кластер.
10. **Зарегистрирована JSON-схема** под субъектом `MyTopic-3`.
11. **Попытка настроить NiFi** на JSON-схему через `JsonRecordSetWriter` —
    неудачно: `SchemaNotFoundException: Failed to retrieve Schema with name
    MyTopic-3 ... failure response message: null`.
12. **Зарегистрирована Avro-схема** под субъектом `MyTopic-3-value`.
13. **NiFi переведён на Avro**: `AvroRecordSetWriter` +
    `Schema Write Strategy = Confluent Schema Registry Reference` +
    `Schema Name = MyTopic-3-value`. Отправка работает.

---

## 11. Ключевой вывод по форматам схем

Главная ошибка в предыдущей версии документа — неверная трактовка причины сбоя.
Правильная картина:

| Компонент | Ожидаемый формат схемы |
|---|---|
| `JsonRecordSetWriter` | **JSON Schema** |
| `AvroRecordSetWriter` | **Avro** |
| `ConfluentSchemaRegistry` (контроллер-сервис) | получает схему из реестра как текст и передаёт writer'у |

Отсюда правило: **формат схемы в реестре должен совпадать с типом Record Writer**.

- `JsonRecordSetWriter` + Avro-схема в реестре → ошибка вида
  «not a valid JSON Schema».
- `AvroRecordSetWriter` + JSON Schema в реестре → ошибка разбора Avro.
- Рабочая комбинация в текущем решении: `AvroRecordSetWriter` +
  субъект `MyTopic-3-value` с `schemaType: "AVRO"`.

Дополнительно: имя субъекта должно соответствовать тому, что указано в
`Schema Name`. Стандартная стратегия именования Confluent — `<топик>-value`
для значения и `<топик>-key` для ключа, что и объясняет выбор
`MyTopic-3-value` вместо `MyTopic-3`.

---

## 12. Открытые вопросы

**Расхождение `brokers_count`.** API возвращает `brokers_count: "1"` при
фактических трёх хостах. Из-за этого ранее возникала ошибка формы
«Значение должно быть меньше или равно 0» при создании топика.
Топики создавать через CLI/API, а не через форму консоли.

**Устаревшая страница консоли.** Вкладка в браузере указывает на кластер
`c9q2qdqmatvf9g3ii9b1`, которого больше не существует. Актуальный адрес:
`https://console.cloud.yandex.ru/folders/b1garhid9ikqi6ek09j9/managed-kafka/cluster/c9q6v0lk049fbhvqe8dh`.

**Неиспользуемый субъект `MyTopic-3`.** JSON-схема осталась в реестре, но
не используется. Её можно удалить, чтобы не путать при разборе инцидентов:

```bash
curl --request DELETE \
  --url 'https://rc1a-6782bojupms8aio6.mdb.yandexcloud.net:443/subjects/MyTopic-3' \
  --user kafka-user:'<пароль>' --cacert ~/YandexInternalRootCA.crt
```
**Чтение Avro-сообщений.** `kafkactl consume` покажет бинарный префикс и
Avro-полезную нагрузку в нечитаемом виде. Для проверки содержимого нужен
клиент с поддержкой Schema Registry либо Kafka UI кластера
(`https://ui-c9q6v0lk049fbhvqe8dh.kafka.yandexcloud.net`).

**Ограничение по документации.** Официальной документации Apache NiFi по
свойствам `AvroRecordSetWriter`, `JsonRecordSetWriter` и
`ConfluentSchemaRegistry` получить не удалось — поиск возвращает только
материалы Yandex Cloud. Названия свойств NiFi приведены по общему знанию
продукта и подтверждены практикой работы в этом решении. Подтверждено
документацией Yandex Cloud: адрес реестра — HTTPS на порту 443 через FQDN
хоста-брокера, авторизация логином/паролем пользователя Kafka, тот же
клиентский SSL-сертификат, что для брокеров, субъекты вида
`<топик>-key` / `<топик>-value`, поддерживаемые форматы Avro, JSON Schema,
Protobuf.

---

## Источники

- [Управление схемами данных — Managed Service for Apache Kafka](https://yandex.cloud/ru/docs/managed-kafka/concepts/managed-schema-registry)
- [Использование Managed Schema Registry через REST API](https://yandex.cloud/ru/docs/tutorials/dataplatform/managed-schema-registry-rest)
- [Подключение к кластеру Apache Kafka](https://yandex.cloud/ru/docs/managed-kafka/operations/connect/clients)
- [Предварительная настройка подключения к кластеру Apache Kafka](https://yandex.cloud/ru/docs/managed-kafka/operations/connect/)
- [Как работать с топиками и разделами кластера](https://yandex.cloud/ru/docs/managed-kafka/operations/cluster-topics)
- [yc managed-kafka topic update](https://yandex.cloud/ru/docs/managed-kafka/cli-ref/topic/update)
- [Managed Service for Apache Kafka — ответы на вопросы](https://yandex.cloud/ru/docs/managed-kafka/qa/)
