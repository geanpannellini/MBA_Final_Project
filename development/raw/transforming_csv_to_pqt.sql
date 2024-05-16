-- Transforming all data files csv to parquet
-- 2018
COPY (SELECT * FROM read_csv('data-airbnb/2018/CSV/4abril2018.csv', AUTO_DETECT=TRUE)) TO '4abril2018.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2018/CSV/5maio2018.csv', AUTO_DETECT=TRUE)) TO '5maio2018.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2018/CSV/7julho2018.csv', AUTO_DETECT=TRUE)) TO '7julho2018.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2018/CSV/8agosto2018.csv', AUTO_DETECT=TRUE)) TO '8agosto2018.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2018/CSV/9setembro2018.csv', AUTO_DETECT=TRUE)) TO '9setembro2018.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2018/CSV/10outubro2018.csv', AUTO_DETECT=TRUE)) TO '10outubro2018.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2018/CSV/11novembro2018.csv', AUTO_DETECT=TRUE)) TO '11novembro2018.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2018/CSV/12dezembro2018.csv', AUTO_DETECT=TRUE)) TO '12dezembro2018.parquet';
-- 2019
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/1janeiro2019.csv', AUTO_DETECT=TRUE)) TO '1janeiro2019.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/2fevereiro2019.csv', AUTO_DETECT=TRUE)) TO '2fevereiro2019.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/3marco2019.csv', AUTO_DETECT=TRUE)) TO '3marco2019.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/4abril2019.csv', AUTO_DETECT=TRUE)) TO '4abril2019.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/5maio2019.csv', AUTO_DETECT=TRUE)) TO '5maio2019.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/6junho2019.csv', AUTO_DETECT=TRUE)) TO '6junho2019.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/7julho2019.csv', AUTO_DETECT=TRUE)) TO '7julho2019.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/8agosto2019.csv', AUTO_DETECT=TRUE)) TO '8agosto2019.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/9setembro2019.csv', AUTO_DETECT=TRUE)) TO '9setembro2019.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/10outubro2019.csv', AUTO_DETECT=TRUE)) TO '10outubro2019.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/11novembro2019.csv', AUTO_DETECT=TRUE)) TO '11novembro2019.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2019/CSV/12dezembro2019.csv', AUTO_DETECT=TRUE)) TO '12dezembro2019.parquet';
-- 2020
COPY (SELECT * FROM read_csv('data-airbnb/2020/CSV/1janeiro2020.csv', AUTO_DETECT=TRUE)) TO '1janeiro2020.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2020/CSV/2fevereiro2020.csv', AUTO_DETECT=TRUE)) TO '2fevereiro2020.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2020/CSV/3marco2020.csv', AUTO_DETECT=TRUE)) TO '3marco2020.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2020/CSV/4abril2020.csv', AUTO_DETECT=TRUE)) TO '4abril2020.parquet';
COPY (SELECT * FROM read_csv('data-airbnb/2020/CSV/5maio2020.csv', AUTO_DETECT=TRUE)) TO '5maio2020.parquet';
