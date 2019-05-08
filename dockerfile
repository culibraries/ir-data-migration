FROM python:3.6

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUNBUFFERED=1
ENV AWS_ACCESS_KEY_ID=None
ENV AWS_SECRET_ACCESS_KEY=None
ENV API_TOKEN=None
ENV AWS_DEFAULT_REGION=us-west-2


WORKDIR /
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt


COPY . /app
WORKDIR /app

CMD ["python", "generateJson.py", "data/20190208cuscholar_inventory.csv"]


