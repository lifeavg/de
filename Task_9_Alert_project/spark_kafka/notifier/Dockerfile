FROM python:3.11.1-alpine

WORKDIR /usr/src/app

COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./notifier.py ./notifier.py

CMD [ "python", "notifier.py"]
