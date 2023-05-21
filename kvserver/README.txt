# Instructions

To run the server:

Option 1:
python kvserver.py

Option 2:
docker build -t kv-server .
docker run -it -p 8000:8000 kv-server



Notes:


docker exec -it 401ea30f115c ls /server

docker build -t gitlab.lrz.de:5005/cdb-23/gr4/ms2/kv-server .
docker push gitlab.lrz.de:5005/cdb-23/gr4/ms2/kv-server
