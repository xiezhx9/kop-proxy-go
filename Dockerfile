FROM shoothzj/compile:go AS build
COPY . /opt/compile
WORKDIR /opt/compile/cmd/it
RUN go build -o kop .

FROM shoothzj/base

COPY --from=build /opt/compile/cmd/it/kop /opt/pulsar/kop/kop

CMD ["/usr/bin/dumb-init", "/opt/pulsar/kop/kop"]
