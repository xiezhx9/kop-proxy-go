#!/bin/bash

export PULSAR_ALLOW_AUTO_TOPIC_CREATION_TYPE=partitioned
export PULSAR_BROKER_ENTRY_METADATA_INTERCEPTORS=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
export PULSAR_EXPOSING_BROKER_ENTRY_METADATA_TO_CLIENT_ENABLED=true
export REMOTE_MODE=false
# generate config
/opt/pulsar/mate/config_gen
# start pulsar standalone
$PULSAR_HOME/bin/pulsar-daemon start standalone -nfw -nss >>$PULSAR_HOME/pulsar.stdout.log 2>>$PULSAR_HOME/pulsar.stderr.log
sleep 30
/opt/pulsar/kop/kop
