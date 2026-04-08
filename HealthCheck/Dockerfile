FROM python:3

LABEL maintainer="jgill391@my.bcit.ca"

RUN mkdir /app

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /app

RUN chown -R nobody:nogroup /app
USER nobody

EXPOSE 8120

ENTRYPOINT ["python3"]
CMD ["app.py"]
