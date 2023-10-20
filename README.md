## FlightAnalyzer

Финальный проект курса по Apache Spark на языке Scala

- Требования
  - Scala - 2.12.15 
  - Apache Spark - 3.2.1

- Запуск
  - Скачиваем файлы [airlines.csv](https://stepik.org/media/attachments/lesson/695919/airlines.csv), 
  [airports.csv](https://stepik.org/media/attachments/lesson/695919/airports.csv), 
  [flights.csv](https://drive.google.com/file/d/1b6XW5BAZxDqlIPQBJmUZ0BQMMtyMjBJq/view?usp=sharing) 
  - Клонируйте репозиторий с проектом
  - A: Если вы запускаете проект из IDE
    - Задайте переменную окружения *PROJECT_ENV=dev*
    - Укажите домашний каталог для скачанных файлов в переменную *path*
    - Переопределите переменные *airlinesPath*, *airportsPath*, *flightsPath* на пути до файлов
    - Запускайте
  - B: Если вы компилируете проект в *.jar*
    - ...
  - C: Если вы используете уже скомпилированный проект из файла *FlightAnalyzer.jar*
    - Просто запустите его указав параметры
    ```
    spark-submit --class com.example.FlightAnalyzer --deploy-mode client --master spark://spark:7077 --verbose --supervise /путь_до_jar_файла /путь_до_airlines.csv /путь_до_airports.csv /путь_до_flights.csv
    ```
