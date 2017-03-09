# pundun-telegraf-output
Pundun's telegraf output plugin

go get github.com/pundunlabs/pundun-telegraf-output

then make telegraf


Configuation file



 # Configuration for Pundun
 [[outputs.pundun]]
     ##Location of server in format host:port
     host = "hostname:8887"
     ##Credentials to connect to pundun
     user = "admin"
     password = "admin"
     database = "table_for_measurement"


