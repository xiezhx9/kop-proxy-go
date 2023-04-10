FROM shoothzj/compile:go AS build
COPY . /opt/compile
WORKDIR /opt/compile/cmd/it
RUN go build -o kop .

FROM ttbb/pulsar:mate

COPY docker-build/scripts /opt/pulsar/kop/scripts

COPY --from=build /opt/compile/cmd/it/kop /opt/pulsar/kop/kop

CMD ["/usr/bin/dumb-init", "bash", "-vx", "/opt/pulsar/kop/scripts/start.sh"]
