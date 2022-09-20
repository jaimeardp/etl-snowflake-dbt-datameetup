docker run --name nifi \
  -p 8443:8443 \
  -d \
  -e SINGLE_USER_CREDENTIALS_USERNAME=<NIFI_USER> \
  -e SINGLE_USER_CREDENTIALS_PASSWORD=<NIFI_PASSWORD> \
  apache/nifi:latest