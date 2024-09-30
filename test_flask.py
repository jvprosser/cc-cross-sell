from flask import Flask
import os

app = Flask(__name__)
@app.route('/')
def index():
    return 'Hello from CML Flask App!'

  
PORT = os.getenv('CDSW_APP_PORT', '8090')
print(f"Port: {PORT}")

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=PORT)
    
    
    
    
#    import http.server
#import socketserver
#import os
#PORT = os.getenv('CDSW_APP_PORT', '8090')
#Handler = http.server.SimpleHTTPRequestHandler
#with socketserver.TCPServer(("127.0.0.1", int(PORT)), Handler) as httpd:
#    print("serving at port", PORT)
#    httpd.serve_forever()
#serving at port 8100