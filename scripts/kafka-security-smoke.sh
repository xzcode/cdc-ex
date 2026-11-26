#!/usr/bin/env bash
# Real Kafka 4.0.0 KRaft TLS/SASL acceptance, using official Maven runtime artifacts.
# Requires Java 21 JDK (including keytool), Maven, Python 3 and OpenSSL; no Docker daemon.
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
chmod 700 "$work"
password="$(openssl rand -hex 24)"
broker_pid=""
mvn=("${MVN:-mvn}" -B -ntp)
if [[ -n "${CDC_MAVEN_SETTINGS:-}" ]]; then mvn+=(-s "$CDC_MAVEN_SETTINGS"); fi
extra=()
if [[ -n "${CDC_TEST_MAVEN_ARGS:-}" ]]; then read -r -a extra <<< "$CDC_TEST_MAVEN_ARGS"; fi
cleanup(){
  rc=$?
  if [[ -n "$broker_pid" ]]; then kill -TERM "$broker_pid" 2>/dev/null || true; wait "$broker_pid" 2>/dev/null || true; fi
  if [[ "$rc" != 0 ]]; then
    python3 - "$work" "$password" <<'PY'
import pathlib,sys
for name in ['dependencies.log','format.log','broker.log','tests.log']:
    path=pathlib.Path(sys.argv[1])/name
    if path.exists():print(name+'\n'+'\n'.join(path.read_text(errors='replace').splitlines()[-60:]).replace(sys.argv[2],'[redacted]'))
PY
  fi
  rm -rf "$work"
  return "$rc"
}
trap cleanup EXIT
cat >"$work/pom.xml" <<'XML'
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion><groupId>com.cdcex.test</groupId><artifactId>kafka-security-fixture</artifactId><version>1</version><dependencies><dependency><groupId>org.apache.kafka</groupId><artifactId>kafka_2.13</artifactId><version>4.0.0</version></dependency><dependency><groupId>org.apache.logging.log4j</groupId><artifactId>log4j-slf4j-impl</artifactId><version>2.24.3</version></dependency></dependencies></project>
XML
"${mvn[@]}" -f "$work/pom.xml" org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies -DincludeScope=runtime -DoutputDirectory="$work/lib" >"$work/dependencies.log" 2>&1
keytool -genkeypair -alias broker -keyalg RSA -keysize 2048 -validity 2 -dname 'CN=cdc-kafka-fixture' \
  -ext SAN=IP:127.0.0.1 -storetype PKCS12 -keystore "$work/broker.p12" -storepass "$password" -keypass "$password" >"$work/keytool.log" 2>&1
keytool -exportcert -rfc -alias broker -keystore "$work/broker.p12" -storepass "$password" -file "$work/broker.crt" >>"$work/keytool.log" 2>&1
keytool -importcert -noprompt -alias broker -storetype PKCS12 -keystore "$work/trust.p12" -storepass "$password" -file "$work/broker.crt" >>"$work/keytool.log" 2>&1
python3 - "$work" "$password" <<'PY'
import base64,pathlib,socket,sys,uuid
root=pathlib.Path(sys.argv[1]);password=sys.argv[2];sockets=[];ports=[]
for _ in range(4):
    s=socket.socket();s.bind(('127.0.0.1',0));sockets.append(s);ports.append(s.getsockname()[1])
internal,tls,sasl,controller=ports
root.joinpath('tls-port').write_text(str(tls));root.joinpath('sasl-port').write_text(str(sasl))
root.joinpath('cluster').write_text(base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('='))
properties={'process.roles':'broker,controller','node.id':'1','controller.quorum.voters':f'1@127.0.0.1:{controller}',
    'listeners':f'PLAINTEXT://127.0.0.1:{internal},SSL://127.0.0.1:{tls},SASL_SSL://127.0.0.1:{sasl},CONTROLLER://127.0.0.1:{controller}',
    'advertised.listeners':f'PLAINTEXT://127.0.0.1:{internal},SSL://127.0.0.1:{tls},SASL_SSL://127.0.0.1:{sasl}',
    'controller.listener.names':'CONTROLLER','listener.security.protocol.map':'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_SSL:SASL_SSL',
    'inter.broker.listener.name':'PLAINTEXT','log.dirs':str(root/'data'),'num.partitions':'1',
    'offsets.topic.replication.factor':'1','transaction.state.log.replication.factor':'1','transaction.state.log.min.isr':'1',
    'group.initial.rebalance.delay.ms':'0','auto.create.topics.enable':'false','delete.topic.enable':'true','num.network.threads':'2','num.io.threads':'2',
    'ssl.keystore.location':str(root/'broker.p12'),'ssl.keystore.password':password,'ssl.key.password':password,'ssl.keystore.type':'PKCS12',
    'ssl.truststore.location':str(root/'trust.p12'),'ssl.truststore.password':password,'ssl.truststore.type':'PKCS12','ssl.client.auth':'none',
    'sasl.enabled.mechanisms':'PLAIN,SCRAM-SHA-256,SCRAM-SHA-512',
    'listener.name.sasl_ssl.plain.sasl.jaas.config':f'org.apache.kafka.common.security.plain.PlainLoginModule required user_cdctest="{password}";',
    'listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config':'org.apache.kafka.common.security.scram.ScramLoginModule required;',
    'listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config':'org.apache.kafka.common.security.scram.ScramLoginModule required;'}
root.joinpath('server.properties').write_text('\n'.join(k+'='+v for k,v in properties.items()))
root.joinpath('server.properties').chmod(0o600)
for s in sockets:s.close()
PY
# A URL-safe base64 Kafka UUID may begin with '-'; attach it to the option so argparse treats it as a value.
java -Xmx128m -cp "$work/lib/*" kafka.tools.StorageTool format --cluster-id="$(cat "$work/cluster")" --config "$work/server.properties" \
  --add-scram "SCRAM-SHA-256=[name=cdctest,password=$password]" --add-scram "SCRAM-SHA-512=[name=cdctest,password=$password]" >"$work/format.log" 2>&1
java -Xms128m -Xmx384m -XX:MaxDirectMemorySize=128m -cp "$work/lib/*" kafka.Kafka "$work/server.properties" >"$work/broker.log" 2>&1 & broker_pid=$!
tls_port="$(cat "$work/tls-port")"; sasl_port="$(cat "$work/sasl-port")"
ready=false
for i in $(seq 1 60); do
  kill -0 "$broker_pid" 2>/dev/null || { echo 'Kafka security fixture exited'; exit 1; }
  if python3 - "$tls_port" "$sasl_port" <<'PY'
import socket,sys
try:
    for port in sys.argv[1:]:
        with socket.create_connection(('127.0.0.1',int(port)),timeout=1):pass
except OSError:raise SystemExit(1)
PY
  then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { echo 'Kafka security listeners did not become ready'; exit 1; }
cd "$repo"
CDC_TEST_KAFKA_TLS_BOOTSTRAP="127.0.0.1:$tls_port" CDC_TEST_KAFKA_SASL_BOOTSTRAP="127.0.0.1:$sasl_port" \
  CDC_TEST_KAFKA_TRUSTSTORE="$work/trust.p12" CDC_TEST_KAFKA_PASSWORD="$password" \
  "${mvn[@]}" "${extra[@]}" -pl cdc-kafka -am -Dtest=KafkaTransportTest -Dsurefire.failIfNoSpecifiedTests=false test >"$work/tests.log" 2>&1
tail -35 "$work/tests.log"
echo 'Kafka 4.0.0 SSL, SASL_SSL PLAIN/SCRAM-256/SCRAM-512 round trips and negative certificate/hostname/password checks passed.'
