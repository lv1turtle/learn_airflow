# UpdateCountryInfo.py

https://restcountries.com API를 통한 세계 나라 정보 DAG 작성

#### 목적
- Full Refresh로 구현하여 DAG가 실행될 때 매번 국가 정보를 읽어오게 함

- Redshift에 테이블 생성 후 적재
  - country -> ["name"]["official"]
  - population -> ["population"]
  - area -> ["area"]
  
- 매주 토요일 오전 6시 30분에 실행되도록 작성

- Docker - Airflow Webserver로 실행
![image](https://github.com/lv1turtle/learn_airflow/assets/32154881/6ecf59e6-4ee3-4ffa-89b8-654904388418)