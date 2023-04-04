FROM python:3.11.1-alpine

WORKDIR /usr/src/app

COPY ./requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY ./my_app/ ./my_app/

CMD [ "uvicorn", "my_app.main:app", "--port", "8005", "--host", "0.0.0.0"]