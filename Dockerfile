FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y \
    g++ \
    libpaho-mqtt-dev \
    nlohmann-json3-dev \
    libsqlite3-dev \
    && apt clean

WORKDIR /build

COPY main.cpp .
COPY c1.cpp .

RUN g++ main.cpp -o c2 -lpaho-mqtt3c -lsqlite3 -pthread
RUN g++ c1.cpp -o c1 -lpaho-mqtt3c -pthread

WORKDIR /app

CMD ["sh", "-c", "/build/c2 & /build/c1"]
