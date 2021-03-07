# DBIS_lab_1

Лабораторна робота №1 

Файли у репозиторію:

docker-compose.yaml -- для запуску PostgreSQL та pgAdmin через Docker
prog_query.py -- код написаний мовою Python, який  виконує запит відповідно до варіанту завдання (до попередньо створеної і заповненої таблиці), 
                 записує його результат у новий csv-файл
result.csv -- результат запиту
time.txt -- файл в якому є дані про час роботи завантаження даних у БД.

Інструкція з запуску:

1. Завантажити даний репозиторій
2. Зайти в створену папку (використовуючи команду cd в терміналі)
3. Завантажити файли за 2019 і 2020 роки з сайту https://zno.testportal.com.ua/opendata 
4. Додати файли в попередньо створену папку csv-файли з даними ЗНО, а саме "Odata2019File.csv" та "Odata2020File.csv"
5. У терміналі ввести docker-compose up -d -- запуск контейнерів з PostgreSQL та pgAdmin
6. Запустити файл prog_query.py 
7. Для моделювання "падіння" бази даних ввести в терміналі docker-compose down
8. Для відновлення з'єднання -- docker-compose up -d
9. Для знищення контейнера після завершення роботи, ввести в терміналі docker-compose down
