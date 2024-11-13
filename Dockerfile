FROM rust:1.82 AS builder

WORKDIR /usr/src/myapp
RUN git clone https://github.com/TheTechRobo/bullseye
WORKDIR /usr/src/myapp/bullseye/client
RUN cargo install --path .

FROM python:3.11-bookworm

# This dockerfile is for the client only

RUN pip3 install --upgrade --no-cache-dir 'cryptography<40'
RUN pip3 install --no-cache-dir yt-dlp requests
RUN pip3 install --no-cache-dir https://github.com/TheTechRobo/chat-downloader/archive/refs/heads/master.zip
RUN pip3 install --no-cache-dir websocket-client warcprox
RUN pip3 install --no-cache-dir typing-extensions
RUN pip3 install --upgrade --no-cache-dir 'cryptography<40'

RUN mkdir -p /data
ENV DATA_DIR="/data"
ENV CURL_CA_BUNDLE=""

COPY --from=builder /usr/local/cargo/bin/bullseye-client /usr/bin/bullseye-client

COPY . /app
WORKDIR /app

ENTRYPOINT ["python3", "-u", "client.py"]
