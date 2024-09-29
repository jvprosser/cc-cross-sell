from flask import Flask
import os

app = Flask(__name__)
print(f"Port: {os.environ['CDSW_APP_PORT']}")
@app.route('/')
def index():
    return 'Hello from CML Flask App!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ['CDSW_APP_PORT']))