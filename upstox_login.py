# from __future__ import print_function
# import time
# # import upstox_client
# # from upstox_client.rest import ApiException
# from pprint import pprint
# import urllib.parse
# import pandas as pd
# import requests
# import sys

# step = 2
# step = int(step)

# def main():

#     client_id = "9c319ab6-d4af-4520-b7f3-05cb7368cae5"  # ABC Upstox APIKEY
#     client_secret = "whx9oqbhkh"                        # ABC Upstox API Secret

#     redirect_uri = 'https://www.google.com' # str |
#     api_version = '2.0' # str | API Version Header
#     grant_type = 'authorization_code'
#     parse_url = urllib.parse.quote(redirect_uri, safe="")

#     if step == 1:
#         uri = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}'
#         print(uri)

#     elif step == 2:
#         uri = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}'
#         print(uri)
#         code = input("Please put code and press enter: ")
#         token_url = "https://api-v2.upstox.com/login/authorization/token"
#         headers = {
#             'accept': 'application/json',
#             'Api-Version': api_version,
#             'Content-Type': 'application/x-www-form-urlencoded',
#         }

#         data = {
#             'code': code,
#             'client_id': client_id,
#             'client_secret': client_secret,
#             'redirect_uri': redirect_uri,
#             'grant_type': grant_type
#         }

#         response = requests.post(token_url, headers=headers, data=data)
#         json_response = response.json()

#         print(json_response)
#         print()
#         print("Copy this:", json_response['access_token'])
#         with open("upstox_access_token.txt", 'w') as file:
#             file.write(json_response['access_token'])
#             print ("Login Successful")

# if __name__ == "__main__":
#     main()


from __future__ import print_function
import urllib.parse
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
import subprocess
import sys

CLIENT_ID = "9c319ab6-d4af-4520-b7f3-05cb7368cae5"  # your API key
CLIENT_SECRET = "whx9oqbhkh"  # keep secret!
REDIRECT_URI = "https://www.google.com"  # << whitelist this in Upstox app
API_VERSION = "2.0"
GRANT_TYPE = "authorization_code"
TOKEN_FILE = "upstox_access_token.txt"

app = Flask(__name__)
CORS(app)  # allow Flutter dev server / emulator to call


def auth_url() -> str:
    """Step-1: build the Upstox login URL."""
    base = "https://api-v2.upstox.com/login/authorization/dialog"
    return f"{base}?response_type=code&client_id={CLIENT_ID}&redirect_uri={urllib.parse.quote(REDIRECT_URI, safe='')}"


def exchange_code(code: str) -> dict:
    token_url = "https://api-v2.upstox.com/login/authorization/token"
    headers = {
        "accept": "application/json",
        "Api-Version": API_VERSION,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "code": code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": GRANT_TYPE,
    }
    resp = requests.post(token_url, headers=headers, data=data)
    resp.raise_for_status()
    js = resp.json()
    if "access_token" not in js:
        raise RuntimeError(f"Unexpected response: {js}")

    with open(TOKEN_FILE, "w") as f:
        f.write(js["access_token"])
    print("Login successful – token saved.")
    return {"access_token": js["access_token"], "expires_in": js.get("expires_in")}


# ---- HTTP endpoints Flutter will hit ---------------------------------
@app.route("/upstox/login-url", methods=["GET"])
def get_login_url():
    return jsonify({"url": auth_url()})


@app.route("/upstox/exchange", methods=["POST"])
def post_exchange():
    code = (request.json or {}).get("code")
    if not code:
        return jsonify({"error": "code missing"}), 400
    try:
        return jsonify(exchange_code(code))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# below is only needed if you use REDIRECT_URI = http://localhost:5000/callback
@app.route("/callback", methods=["GET"])
def callback():
    code = request.args.get("code")
    subprocess.run([sys.executable,"algo_server.py"],capture_output=True, text=True)
    print("File Ran")
    return f"<h3>Login OK</h3><p>You can copy this code back to the app: <code>{code}</code></p>"


if __name__ == "__main__":
    # Run: python upstox_service.py
    # On Android-emulator the host is 10.0.2.2, iOS-simulator can use localhost.
    app.run(host="0.0.0.0", port=5000, debug=True)
