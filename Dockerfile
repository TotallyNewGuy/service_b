FROM python:3.10
WORKDIR /app
COPY . /app/
COPY requirements.txt /app
RUN pip install -r requirements.txt
ENTRYPOINT [ "python3", "main.py" ]