FROM python:3.7

# supervisord setup                       
RUN apt-get update && apt-get install -y supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Airflow / Python setup                       
ENV AIRFLOW_HOME=/app/airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN mkdir -p $AIRFLOW_HOME/scripts/
ADD lib $AIRFLOW_HOME/scripts/lib
COPY /dags/main_dag.py $AIRFLOW_HOME/dags/
COPY airflow.cfg $AIRFLOW_HOME/
RUN airflow db init
RUN airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin   
EXPOSE 8080

CMD ["/usr/bin/supervisord"]