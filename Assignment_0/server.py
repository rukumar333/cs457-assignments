# from http.server import HTTPServer, BaseHTTPRequestHandler
import http.server
import socketserver

PORT = 8080

times_accessed = dict()

class MyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path in times_accessed:
            times_accessed[self.path] = times_accessed[self.path] + 1
        else:
            times_accessed[self.path] = 1
        print(self.path, '|',
              self.client_address[0], '|',
              self.client_address[1], '|',
              times_accessed[self.path])
        return http.server.SimpleHTTPRequestHandler.do_GET(self)

Handler = MyHTTPRequestHandler

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print("Serving at port", PORT)
    httpd.serve_forever()
# def run(server_class=HTTPServer, handler_class=BaseHTTPRequestHandler):
#     server_address = ('', PORT)
#     httpd = server_class(server_address, handler_class)
#     httpd.serve_forever()

# run()
