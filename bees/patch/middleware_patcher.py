# -*- coding: utf-8 -*-

# keystonemiddleware
# - keystoneauth1
# - keystoneclient


from keystoneauth1.session import Session as keystoneauth1_Session
from .keystoneauth1 import request_wrapper as r1


# keystonemiddleware
def mock_session_request():
    setattr(keystoneauth1_Session, 'request', r1)
