RUN THE Consumer locally
  npm install
  node app.js <message_hub_rest_endpoint> <message_hub_api_key>

RUN THE PRODUCER in BLUEMIX
  cf login
  cf push <intakeconsumer>
