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

docker build -t gitlab.lrz.de:5005/cdb-23/gr4/ms3/kv-server .
docker push gitlab.lrz.de:5005/cdb-23/gr4/ms3/kv-server

docker build -t gitlab.lrz.de:5005/cdb-23/gr4/ms3/ecs-server .
docker push gitlab.lrz.de:5005/cdb-23/gr4/ms3/ecs-server


docker run --rm -p 37959:37959 --name ms3-gr4-kv-server0 --network ms3/gr4 ms3/gr4/kv-server -a ms3-gr4-kv-server0 -p 37959 -ll INFO -d process_data -s FIFO -c 10 -b ms3-gr4-ecs-server:39187
python kvserver.py -a 127.0.0.1 -p 37959 -ll INFO -d process_data -s FIFO -c 10 -b 127.0.0.1:39187