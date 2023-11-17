"""
Shared utilities for managing TLS keys and certs
"""

# Standard
import base64
import datetime

# Third Party
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

# First Party
import alog

log = alog.use_channel("TLS")

DEFAULT_COMMON_NAME = "oper8.org"


def get_subject(common_name: str = DEFAULT_COMMON_NAME) -> x509.Name:
    """Get the subject object used when creating self-signed certificates. This
    will be consistent across all components, but will be tailored to the domain
    of the cluster.

    Args:
        common_name:  str
            The Common Name to use for this subject

    Returns:
        subject:  x509.Name
            The full subect object to use when constructing certificates
    """
    return x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ]
    )


def generate_key(encode=True):
    """Generate a new RSA key for use when generating TLS components

    Args:
        encode:  bool
            Base64 encode the output pem bytes

    Returns:
        key:  RSAPrivateKey
            The key object that can be used to sign certificates
        key_pem:  str
            The PEM encoded string for the key
    """
    key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    key_pem = key.private_bytes(
        Encoding.PEM,
        PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return (key, (base64.b64encode(key_pem) if encode else key_pem).decode("utf-8"))


def generate_ca_cert(key, encode=True):
    """Generate a Certificate Authority certificate based on a private key

    Args:
        key:  RSAPrivateKey
            The private key that will pair with this CA cert
        encode:  bool
            Base64 encode the output pem bytes

    Returns:
        ca:  str
            The PEM encoded string for this CA cert
    """

    # Create self-signed CA
    # The specifics of the extensions that are required for the CA were gleaned
    # from the etcd operator example found here:
    # https://github.com/openshift/etcd-ha-operator/blob/master/roles/tls_certs/templates/ca_crt_conf.j2
    log.debug("Creating CA")
    subject = get_subject()
    ca = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(
            # Our certificate will be valid for 10000 days
            datetime.datetime.utcnow()
            + datetime.timedelta(days=10000)
        )
        .add_extension(
            # X509v3 Basic Constraints: critical
            #     CA:TRUE
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True,
        )
        .add_extension(
            # X509v3 Key Usage: critical
            #     Digital Signature, Key Encipherment, Certificate Sign
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256(), default_backend())
    )

    cert_pem = ca.public_bytes(Encoding.PEM)
    return (base64.b64encode(cert_pem) if encode else cert_pem).decode("utf-8")


def generate_derived_key_cert_pair(ca_key, san_list, encode=True, key_cert_sign=False):
    """Generate a certificate for use in encrypting TLS traffic, derived from
    a common key

    Args:
        key:  RSAPrivateKey
            The private key that will pair with this CA cert
        san_list:  list(str)
            List of strings to use for the Subject Alternate Name
        encode:  bool
            Whether or not to base64 encode the output pem strings
        key_cert_sign:  bool
            Whether or not to set the key_cert_sign usage bit in the generated certificate.
            This may be needed when the derived key/cert will be used as an intermediate CA
            or expected to act as a self-signed CA.
            Reference: https://ldapwiki.com/wiki/KeyUsage

    Returns:
        key_pem:  str
            The pem-encoded key (base64 encoded if encode set)
        crt_pem:  str
            The pem-encoded cert (base64 encoded if encode set)
    """

    # Create a new private key for the server
    key, key_pem = generate_key(encode=encode)

    # Create the server certificate as if using a CSR. The final key will be
    # signed by the CA private key, but will have the public key from the
    # server's key.
    #
    # NOTE: It is not legal to use an identical Common Name for both the CA and
    #   the derived certificate. With openssl 1.1.1k, this results in an invalid
    #   certificate that fails with "self signed certificate."
    #   CITE: https://stackoverflow.com/a/19738223
    cert = (
        x509.CertificateBuilder()
        .subject_name(get_subject(f"{DEFAULT_COMMON_NAME}.server"))
        .issuer_name(get_subject())
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(
            # Our certificate will be valid for 10000 days
            datetime.datetime.utcnow()
            + datetime.timedelta(days=10000)
        )
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName(san) for san in san_list]),
            critical=False,
        )
        .add_extension(
            # X509v3 Key Usage: critical
            #     Digital Signature, Key Encipherment
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=key_cert_sign,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            # X509v3 Extended Key Usage:
            #     TLS Web Client Authentication, TLS Web Server Authentication
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256(), default_backend())
    )

    crt_pem = cert.public_bytes(Encoding.PEM)
    return (key_pem, (base64.b64encode(crt_pem) if encode else crt_pem).decode("utf-8"))


def parse_private_key_pem(key_pem):
    """Parse the content of a pem-encoded private key file into an RSAPrivateKey

    Args:
        key_pem:  str
            The pem-encoded key (not base64 encoded)

    Returns:
        key:  RSAPrivateKey
            The parsed key object which can be used for signing certs
    """
    return serialization.load_pem_private_key(key_pem.encode("utf-8"), None)


def parse_public_key_pem_from_cert(cert_pem):
    """Extract the pem-encoded public key from a pem-encoded

    Args:
        cert_pem:  str
            The pem-encoded certificate (not base64 encoded)

    Returns:
        key:  RSAPrivateKey
            The parsed key object which can be used for signing certs
    """
    return (
        x509.load_pem_x509_certificate(cert_pem.encode("utf-8"))
        .public_key()
        .public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        .decode("utf-8")
    )
