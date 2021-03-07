"""Лабораторна робота №1
Федорів О. , КМ-83
Варіант 11
Порівняти середній бал з Фізики у кожному регіоні
у 2020 та 2019 роках
серед тих, кому було зараховано тест
"""

import csv
import psycopg2
import psycopg2.errorcodes
import itertools
import time
import datetime

# підключення до бази даних
def create_connection(db_name, db_user, db_password, db_host):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port="5432"
        )
        print("З'єднання з базою даних  успішне")
    except:
        pass
    return connection

conn = create_connection("postgres", "postgres", "admin", "localhost")
cursor = conn.cursor()

# видалення таблиці, якщо така існує
cursor.execute('DROP TABLE IF EXISTS zno_data;')
conn.commit()


def create_table():

    with open("Odata2019File.csv", "r", encoding="cp1251") as csv_file:


#        header = '"OUTID";"Birth";"SEXTYPENAME";"REGNAME";"AREANAME";"TERNAME";"REGTYPENAME";"TerTypeName";"ClassProfileNAME";"ClassLangName";"EONAME";"EOTYPENAME";"EORegName";"EOAreaName";"EOTerName";"EOParent";"UkrTest";"UkrTestStatus";"UkrBall100";"UkrBall12";"UkrBall";"UkrAdaptScale";"UkrPTName";"UkrPTRegName";"UkrPTAreaName";"UkrPTTerName";"histTest";"HistLang";"histTestStatus";"histBall100";"histBall12";' \
#                 '"histBall";"histPTName";"histPTRegName";"histPTAreaName";"histPTTerName";"mathTest";"mathLang";"mathTestStatus";"mathBall100";"mathBall12";"mathBall";"mathPTName";"mathPTRegName";"mathPTAreaName";"mathPTTerName";"physTest";"physLang";"physTestStatus";"physBall100";' \
#                 '"physBall12";"physBall";"physPTName";"physPTRegName";"physPTAreaName";"physPTTerName";"chemTest";"chemLang";"chemTestStatus";"chemBall100";"chemBall12";"chemBall";"chemPTName";"chemPTRegName";"chemPTAreaName";"chemPTTerName";"bioTest";"bioLang";"bioTestStatus";"bioBall100";"bioBall12";"bioBall";"bioPTName";"bioPTRegName";' \
#                 '"bioPTAreaName";"bioPTTerName";"geoTest";"geoLang";"geoTestStatus";"geoBall100";"geoBall12";"geoBall";"geoPTName";"geoPTRegName";"geoPTAreaName";"geoPTTerName";"engTest";"engTestStatus";"engBall100";"engBall12";"engDPALevel";"engBall";"engPTName";"engPTRegName";"engPTAreaName";"engPTTerName";"fraTest";"fraTestStatus";"fraBall100";"fraBall12";"fraDPALevel";"fraBall";"fraPTName";"fraPTRegName";"fraPTAreaName";"fraPTTerName";"deuTest";"deuTestStatus";"deuBall100";"deuBall12";"deuDPALevel";"deuBall";' \
#                 '"deuPTName";"deuPTRegName";"deuPTAreaName";"deuPTTerName";"spaTest";"spaTestStatus";"spaBall100";"spaBall12";"spaDPALevel";"spaBall";"spaPTName";"spaPTRegName";"spaPTAreaName";"spaPTTerName"'

        header = csv_file.readline()
        header = header.split(';')
        header = [elem.strip('"') for elem in header]
        columns = "\n\tYear INT,"
        header[-1] = header[-1].rstrip('"\n')


        for elem in header:
            if 'Ball' in elem: columns += '\n\t' + elem + ' REAL,'
            elif elem == 'Birth': columns += '\n\t' + elem + ' INT,'
            elif elem == "OUTID": columns += '\n\t' + elem + ' VARCHAR(40) PRIMARY KEY,'
            else: columns += '\n\t' + elem + ' VARCHAR(255),'
        create_table_query = '''CREATE TABLE IF NOT EXISTS zno_data (''' + columns.rstrip(',') + '\n);'
        cursor.execute(create_table_query)
        conn.commit()
        return header

header = create_table()



def insert_data(f, year, conn, cursor, time_file):


    # Починаємо відлік часу на обродку даних з файлів
    start_time = time.time()

    #відкриваємо файл та считуємо дані через ";" считуємо дані порціями по 50 рядків
    with open(f, "r", encoding="cp1251") as csv_file:
        print('Виконується читання файлу... '+f )
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        batches_inserted = 0
        size_b = 50
        inserted_all = False

        # виконуємо цикл поки не вставили всі рядки
        while not inserted_all:
            try:
                insert_query = '''INSERT INTO zno_data (year, ''' + ', '.join(header) + ') VALUES '
                count = 0
                for row in csv_reader:
                    count += 1

        # обробляємо запис, для знаходження середнього необхідний запис чисел через крапку
                    for i in row:

                        # пропускаємо порожні комірки
                        if row[i] == 'null':
                            pass

                        # текстові значення беремо в одинарні лапки
                        elif i.lower() != 'birth' and 'ball' not in i.lower():
                            row[i] = "'" + row[i].replace("'", "''") + "'"

                        # в числових значеннях замінюємо кому на крапку
                        elif 'ball100' in i.lower():
                            row[i] = row[i].replace(',', '.')
                    insert_query += '\n\t(' + str(year) + ', ' + ','.join(row.values()) + '),'
                    if count == size_b:
                        count = 0
                        insert_query = insert_query.rstrip(',') + ';'
                        cursor.execute(insert_query)
                        conn.commit()
                        batches_inserted += 1
                        insert_query = '''INSERT INTO zno_data (year, ''' + ', '.join(header) + ') VALUES '



                # якщо досягли кінця файлу
                if count != 0:
#                    insert_query = insert_query.rstrip(',') + ';'
                    cursor.execute(insert_query.rstrip(',') + ';')
                    conn.commit()
                inserted_all = True



            except psycopg2.OperationalError as err:
                if err.pgcode == psycopg2.errorcodes.ADMIN_SHUTDOWN:

                    print("Відбулося падіння бази даних -- очікуйте відновлення з'єднання...")
                    t=datetime.datetime.now()
                    time_file.write(str(t) + " - втрата з'єднання\n")

                    connection_restored = False
                    while not connection_restored:
                        try:

                            # намагаємось підключитись до бази даних
                            # за допомогою попередньо написаної функції  'create_connection'
                            conn = create_connection("postgres", "postgres", "admin", "localhost")
                            cursor = conn.cursor()

                            time_file.write(str(t) + " - відновлення з'єднання\n")
                            connection_restored = True
                        # у випадку помилки OperationalError
                        except psycopg2.OperationalError:
                            pass

                    print("З'єднання відновлено!")
                    csv_file.seek(0,0)
                    delimiter_csv = csv.DictReader(csv_file, delimiter=';')
                    size = batches_inserted * size_b

                    csv_reader = itertools.islice(delimiter_csv, size , None)




    #Записуємо у файл скільки часу було витрачено на обробку даних із файлів
    end_time = time.time() - start_time
    time_file.write(str(end_time) + "сек. - файл "+ f + " оброблено\n")

    return conn, cursor

# відкриваємо файл для того щоб здійснити запис
time_file = open('time.txt', 'w')


# під час виклику функції реалізовується запис тривалості опрацювання у файл time.txt
# окремо 2019 і 2020 роки
conn, cursor = insert_data("Odata2019File.csv", 2019, conn, cursor, time_file)
conn, cursor = insert_data("Odata2020File.csv", 2020, conn, cursor, time_file)


# Закриваємо файл після запису
time_file.close()



# виконання завдання відповідно до варіанту
QUERY = '''
SELECT regname AS "Область", year AS "Рік", avg(physBall100) AS "Середній бал"
FROM zno_data
WHERE physTestStatus = 'Зараховано'
GROUP BY regname, year
ORDER BY year, avg(physBall100) DESC;
'''
cursor.execute(QUERY)


# запис результату виконаного завдання у csv файл
with open('result.csv', 'w', encoding="utf-8") as result_csv:
    csv_writer = csv.writer(result_csv)
    header_row = ['Область', 'Рік', 'Середній бал з фізики']

    csv_writer.writerow(header_row)
    for row in cursor:
        csv_writer.writerow(row)


cursor.close()
conn.close()
