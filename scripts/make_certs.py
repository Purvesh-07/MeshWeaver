import pathlib
import subprocess
import sys

out = pathlib.Path('certs')
out.mkdir(exist_ok=True)


def sh(*args):
    subprocess.run(args, check=True)


sh('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
   '-keyout', str(out / 'ca.key'), '-out', str(out / 'ca.crt'),
   '-days', '365', '-subj', '/CN=meshweaver-ca')

for name in sys.argv[1:] or ['node1', 'node2', 'node3']:
    sh('openssl', 'req', '-newkey', 'rsa:2048', '-nodes',
       '-keyout', str(out / (name + '.key')), '-out', str(out / (name + '.csr')),
       '-subj', '/CN=' + name)
    sh('openssl', 'x509', '-req', '-in', str(out / (name + '.csr')),
       '-CA', str(out / 'ca.crt'), '-CAkey', str(out / 'ca.key'), '-CAcreateserial',
       '-out', str(out / (name + '.crt')), '-days', '365')
print('certificates written to', out)
