#!/usr/bin/env bash
# Package the binary that CI tested. Never rebuild or replace that binary here.
# Usage: package-release.sh [tested.jar] [output-directory] [bom.json] [build.json]
set -euo pipefail
umask 022
root=$(git rev-parse --show-toplevel)
cd "$root"
version=$(cat VERSION)
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$ ]] || { echo 'Invalid VERSION' >&2; exit 1; }
jar=$(realpath "${1:-cdc-server/target/cdc-server-$version.jar}")
destination=$(realpath -m "${2:-dist/release}")
bom=$(realpath "${3:-target/bom.json}")
metadata=$(realpath "${4:-target/build.json}")
commit=$(git rev-parse HEAD)
[[ "$commit" == "${CDC_RELEASE_SOURCE_SHA:-$commit}" ]] || { echo 'Checkout differs from the tested commit' >&2; exit 1; }
git diff --quiet HEAD -- || { echo 'Release packaging requires unchanged tracked source files' >&2; exit 1; }
[[ ! -e "$destination" ]] || { echo 'Release destination already exists; refusing to overwrite' >&2; exit 1; }
[[ -f docs/RELEASE_NOTES.md ]] || { echo 'Release notes are required' >&2; exit 1; }
mkdir -p "$(dirname "$destination")"
stage=$(mktemp -d "$(dirname "$destination")/.cdcex-release.XXXXXX")
cleanup() { [[ -z "$stage" ]] || rm -rf "$stage"; }
trap cleanup EXIT

python3 - "$jar" "$bom" "$metadata" "$stage" "$version" "$commit" <<'PY'
import hashlib, io, json, os, pathlib, re, shutil, sys, zipfile
jar, bom, metadata, folder = map(pathlib.Path, sys.argv[1:5])
version, commit = sys.argv[5:]
prefix = f'cdc-ex-{version}'
def digest(data): return hashlib.sha256(data).hexdigest()
def require(condition, message):
    if not condition: raise SystemExit(message)
def properties(data):
    return dict(line.split('=', 1) for line in data.decode('ISO-8859-1').splitlines()
                if '=' in line and not line.startswith(('#', '!')))

build = json.loads(metadata.read_text())
require(build.get('commit') == commit and build.get('version') == version,
        'Build metadata does not match the checked-out commit and VERSION')
expected_run = os.environ.get('CDC_BUILD_RUN_ID')
require(not expected_run or str(build.get('workflowRunId')) == expected_run,
        'Artifact was not built by the expected workflow run')
jar_hash = digest(jar.read_bytes())
require(build.get('jarSha256') == jar_hash, 'Tested JAR checksum mismatch')
require(build.get('sbomSha256') == digest(bom.read_bytes()), 'Tested SBOM checksum mismatch')
sbom = json.loads(bom.read_text())
require(sbom.get('bomFormat') == 'CycloneDX' and sbom.get('specVersion') == '1.6',
        'Expected CycloneDX 1.6 JSON from the pinned Maven plugin')
require(sbom.get('metadata', {}).get('component', {}).get('version') == version,
        'SBOM version differs from VERSION')
components = sbom.get('components', [])
require(isinstance(components, list) and len(components) >= 30, 'SBOM dependency inventory is incomplete')
required = {'spring-boot', 'kafka-clients', 'debezium-embedded', 'sqlite-jdbc',
            'mysql-connector-j', 'mariadb-java-client', 'postgresql', 'mongodb-driver-sync'}
require(required <= {c.get('name') for c in components}, 'SBOM is missing required runtime dependencies')
coordinates = {}
for component in components:
    if component.get('type') == 'library' and component.get('name') and component.get('version'):
        filename = f"{component['name']}-{component['version']}.jar"
        coordinates.setdefault(filename, []).append(component)

with zipfile.ZipFile(jar) as archive:
    require(archive.testzip() is None, 'Corrupt tested JAR')
    names = archive.namelist()
    require(len(names) == len(set(names)), 'Duplicate paths in tested JAR')
    app = properties(archive.read('META-INF/maven/com.cdcex/cdc-server/pom.properties'))
    require(app == {'groupId': 'com.cdcex', 'artifactId': 'cdc-server', 'version': version},
            'Packaged Maven identity differs from VERSION')
    manifest = archive.read('META-INF/MANIFEST.MF').decode('UTF-8')
    require('Start-Class: com.cdcex.server.CdcApplication' in manifest,
            'JAR is not the executable CDC EX distribution')
    require('BOOT-INF/classes/static/index.html' in names, 'Packaged management UI is missing')
    assets = [name for name in names if name.startswith('BOOT-INF/classes/static/assets/') and not name.endswith('/')]
    require(any(name.endswith('.js') for name in assets), 'Packaged management UI JavaScript is missing')
    entries = []
    for name in sorted(n for n in names if n.startswith('BOOT-INF/lib/') and n.endswith('.jar')):
        data = archive.read(name)
        candidates = coordinates.get(pathlib.PurePosixPath(name).name, [])
        item = {'path': name, 'bytes': len(data), 'sha256': digest(data)}
        if len(candidates) == 1:
            component = candidates[0]
            item.update(groupId=component.get('group'), artifactId=component['name'],
                        version=component['version'], coordinateSource='maven-cyclonedx')
        else:
            with zipfile.ZipFile(io.BytesIO(data)) as nested:
                poms = [p for p in nested.namelist() if p.startswith('META-INF/maven/') and p.endswith('/pom.properties')]
                if len(poms) == 1:
                    item.update(properties(nested.read(poms[0])))
                    item['coordinateSource'] = 'embedded-maven-properties'
                else:
                    # Spring Boot adds this tooling JAR at repackage time, outside Maven's application graph.
                    match = re.fullmatch(r'spring-boot-jarmode-tools-([0-9][A-Za-z0-9.+-]*)\.jar', pathlib.PurePosixPath(name).name)
                    require(match is not None, f'Cannot identify packaged dependency: {name}')
                    require(f'Spring-Boot-Version: {match[1]}' in manifest, 'Boot tooling version mismatch')
                    item.update(groupId='org.springframework.boot', artifactId='spring-boot-jarmode-tools',
                                version=match[1], coordinateSource='spring-boot-repackage')
        require(all(item.get(key) for key in ('groupId', 'artifactId', 'version')),
                f'Incomplete packaged dependency coordinates: {name}')
        entries.append(item)
    require(len(entries) >= 30, 'Executable JAR dependency inventory is incomplete')
    inventory = {
        'format': 'cdc-ex-packaged-runtime-v1', 'version': version, 'commit': commit,
        'jarSha256': jar_hash, 'sbomSha256': digest(bom.read_bytes()),
        'scope': 'Exact BOOT-INF/lib contents. Maven graph is in the companion CycloneDX SBOM; Boot repackage tooling is explicitly identified here.',
        'dependencies': entries,
        'frontend': {
            'lockfileSha256': digest(pathlib.Path('cdc-web/package-lock.json').read_bytes()),
            'scope': 'Built frontend assets; the source archive contains the complete npm lockfile, including build dependencies.',
            'assets': [{'path': name, 'sha256': digest(archive.read(name))} for name in sorted(assets)]
        }
    }
    (folder / f'{prefix}-runtime-dependencies.json').write_text(json.dumps(inventory, indent=2, sort_keys=True) + '\n')

shutil.copyfile(jar, folder / f'{prefix}.jar')
shutil.copyfile(bom, folder / f'{prefix}-sbom.cdx.json')
shutil.copyfile(metadata, folder / f'{prefix}-build.json')
shutil.copyfile('docs/RELEASE_NOTES.md', folder / 'RELEASE_NOTES.md')
report = os.environ.get('CDC_ACCEPTANCE_REPORT')
if report:
    acceptance = pathlib.Path(report)
    result = json.loads(acceptance.read_text())
    require(result.get('keyedPayloadSha256Equal') is True and
            all(result.get(key) == 'passed' for key in ('hardCrashRecovery', 'schemaDriftAndRepair', 'expiredBinlogBlocks')),
            'Acceptance report does not record the required passed assertions')
    shutil.copyfile(acceptance, folder / f'{prefix}-acceptance.json')
print(f'Validated tested JAR, {len(entries)} packaged Maven dependencies and {len(assets)} frontend assets.')
PY

git archive --format=tar --prefix="cdc-ex-$version/" "$commit" | gzip -n >"$stage/cdc-ex-$version-source.tar.gz"
python3 - "$stage" "$version" "$commit" <<'PY'
import hashlib, pathlib, subprocess, sys, tarfile
folder, version, commit = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
with tarfile.open(folder / f'cdc-ex-{version}-source.tar.gz', 'r:gz') as archive:
    expected = {f'cdc-ex-{version}/{name}' for name in ('VERSION', 'pom.xml', 'Dockerfile', 'cdc-web/package-lock.json', 'scripts/package-release.sh', 'docs/RELEASE_NOTES.md')}
    if not expected <= set(archive.getnames()): raise SystemExit('Source archive is incomplete')
    tracked = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', '-z', commit]).decode().rstrip('\0').split('\0')
    if not {f'cdc-ex-{version}/{name}' for name in tracked} <= set(archive.getnames()):
        raise SystemExit('Source archive does not contain every tracked file')
    if archive.extractfile(f'cdc-ex-{version}/VERSION').read().decode().strip() != version:
        raise SystemExit('Source archive version differs from the tested binary')
lines = [hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + path.name
         for path in sorted(folder.iterdir()) if path.is_file()]
(folder / 'SHA256SUMS').write_text('\n'.join(lines) + '\n')
PY
(cd "$stage" && sha256sum --check SHA256SUMS)
mv -T "$stage" "$destination"
stage=''
printf 'Release artifacts prepared at %s\n' "$destination"
