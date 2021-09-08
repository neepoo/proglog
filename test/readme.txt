ca-csr.json
cfssl will use `ca-csr.json` to configure our CA’s certificate
C—country
L—locality or municipality (such as city)
ST—state or province
O—organization
OU—organizational unit (such as the department responsible for
owning the key)


ca-config.json
{"signing": {
 "profiles": {
 "server": {
 "expiry": "8760h",
 "usages": [
 "signing",
 "key encipherment",
 "server auth"
 ]
 },
 "client": {
 "expiry": "8760h",
 "usages": [
 "signing",
 "key encipherment",
 "client auth"
 ]
 }
 }
 }
}
Our CA needs to know what kind of certificates it will issue
 The signing
section of this configuration file defines your CA’s signing policy. Our
configuration file says that the CA can generate client and server
certificates that will expire after a year and the certificates may be used
for digital signatures, encrypting keys, and auth.


server-csr.json
cfssl will use these configs to configure our server’s certificate. The
“hosts” field is a list of the domain names that the certificate should be
valid for. Since we’re running our service locally, we just need 127.0.0.1
and localhost.
