# Databricks notebook source
import os
from flask import Flask, request
from pyngrok import ngrok

app = Flask(__name__)

@app.route('/uplift-webhook-receiver-test', methods=['POST'])
def webhook():
    data = request.get_json()
    # Do something with the data 
    print(data)
    return '', 200

if __name__ == '__main__':
    # Set up a tunnel on port 5000 for our Flask object to interact locally
    public_url = ngrok.connect(addr='5000', proto='http', name='uplift-webhook-receiver').public_url
    #print(' * Tunnel URL:', public_url)
    print(" * ngrok tunnel \"{}\" -> \"http://127.0.0.1:{}\"".format(public_url, '5000'))
    # Start the Flask app
    app.run(debug=False, host='127.0.0.1', port=5000)

# COMMAND ----------

tunnels = ngrok.get_tunnels()
print(tunnels)

# COMMAND ----------

ngrok.disconnect("https://8e65-35-155-15-56.ngrok.io")

# COMMAND ----------


