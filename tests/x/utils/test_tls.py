"""
Test the TLS utility functionality
"""

# Third Party
from cryptography import x509

# First Party
import alog

# Local
from oper8.x.utils import tls

log = alog.use_channel("TEST")


def test_get_subject_valid_type():
    """Make sure the type returned by get_subject is the right type"""
    subject = tls.get_subject()
    assert isinstance(subject, x509.Name)
