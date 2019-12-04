import xmlrpc.client

client = xmlrpc.client.ServerProxy("http://" + "localhost:9001")

# Test ping
if client.surfstore.ping():
   print("Ping() successful")