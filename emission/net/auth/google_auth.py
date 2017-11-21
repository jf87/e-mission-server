import logging
import json
import traceback
import requests

# For decoding JWTs on the client side
from google.oauth2 import id_token
from google.auth.transport import requests

class GoogleAuthMethod:
    def __init__(self):
        key_file = open('conf/net/auth/google_auth.json')
        key_data = json.load(key_file)
        self.client_key = key_data["client_key"]
        self.client_key_old = key_data["client_key_old"]
        self.ios_client_key = key_data["ios_client_key"]
        self.ios_client_key_new = key_data["ios_client_key_new"]
        self.valid_keys = [self.client_key, self.client_key_old,
            self.ios_client_key, self.ios_client_key_new]

    # Code snippet from 
    # https://developers.google.com/identity/sign-in/android/backend-auth
    def __verifyTokenFields(tokenFields, audienceKey, issKey):
        if audienceKey not in tokenFields:
            raise ValueError("Invalid token %s, does not contain %s" % 
                (tokenFields, audienceKey))
        in_client_key = tokenFields[audienceKey]
        if in_client_key not in self.valid_keys:
                raise ValueError("Incoming client key %s not in valid list %s" % 
                    (in_client_key, self.valid_keys))

        if 'iss' not in tokenFields:
            raise ValueError("Invalid token %s" % tokenFields)

        in_issuer = idinfo['iss'] 
        issuer_valid_list = ['accounts.google.com', 'https://accounts.google.com']
        if in_issuer not in issuer_valid_list:
            raise ValueError('Wrong issuer %s, expected %s' % (in_issuer, issuer_valid_list))

        return tokenFields['email']
 

    def verifyUserToken(self, token):
        try:
            # attempt to validate token on the client-side
            logging.debug("Using the google auth library to verify id token of length %d from android phones" % len(token))
            tokenFields = id_token.verify_oauth2_token(token, requests.Request())
            logging.debug("tokenFields from library = %s" % tokenFields)
            verifiedEmail = _verifyTokenFields(tokenFields, "aud", "iss")
            logging.debug("Found user email %s" % tokenFields['email'])
            return verifiedEmail
        except ValueError:
            logging.debug("OAuth failed to verify id token, falling back to constructedURL")
            #fallback to verifying using Google API
            constructedURL = ("https://www.googleapis.com/oauth2/v1/tokeninfo?id_token=%s" % token)
            r = requests.get(constructedURL)
            tokenFields = json.loads(r.content)
            logging.debug("tokenFields from constructedURL= %s" % tokenFields)
            verifiedEmail = _verifyTokenFields(tokenFields, "audience", "iss")
            logging.debug("Found user email %s" % tokenFields['email'])
            return verifiedEmail

