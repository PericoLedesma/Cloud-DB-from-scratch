# Instructions

To run the server:

docker build -t gitlab.lrz.de:5005/cdb-23/gr4/ms2/kv-server .
docker push gitlab.lrz.de:5005/cdb-23/gr4/ms2/kv-server

Option 1:
python CLI_client.py -a 127.0.0.1 -p 37957
python CLI_client.py -a 127.0.0.1 -p 37958
python CLI_client.py -a 127.0.0.1 -p 37959

python kvserver.py -i 0 -a 127.0.0.1 -p 37959
python kvserver.py -i 1 -a 127.0.0.1 -p 37958
python kvserver.py -i 2 -a 127.0.0.1 -p 37957

python ecs.py -a 0.0.0.0 -p 40823 -ll FINEST

Option 2:
docker build -t kv-client .
docker run --rm -it -p 6000:6000 --name client --network ms3/gr4 kv-client -a ms3-gr4-kv-server0 -p 37959

docker build -t ms3/gr4/kv-server .
docker run --rm -it -p 37959:37959 --name ms3-gr4-kv-server0 --network ms3/gr4 ms3/gr4/kv-server -a ms3-gr4-kv-server0 -p 37959 -ll INFO -d process_data -s FIFO -c 10 -b ms3-gr4-ecs-server:40823

docker build -t ms3/gr4/kv-server .
docker run --rm -it -p 37999:37999 --name ms3-gr4-kv-server0 --network ms3/gr4 ms3/gr4/kv-server -a ms3-gr4-kv-server0 -p 37999 -ll INFO -d process_data -s FIFO -c 10 -b ms3-gr4-ecs-server:40823


docker build -t ms3/gr4/ecs-server .
docker run --rm -it -p 40823:40823 --name ms3-gr4-ecs-server --network ms3/gr4 ms3/gr4/ecs-server -a 0.0.0.0 -p 40823 -ll FINEST



Notes:
docker exec -it 401ea30f115c ls /server


### MS3
docker build -t gitlab.lrz.de:5005/cdb-23/gr4/ms3/kv-server .
docker push gitlab.lrz.de:5005/cdb-23/gr4/ms3/kv-server

docker build -t gitlab.lrz.de:5005/cdb-23/gr4/ms3/ecs-server .
docker push gitlab.lrz.de:5005/cdb-23/gr4/ms3/ecs-server

### MS4
docker build -t gitlab.lrz.de:5005/cdb-23/gr4/ms4/kv-server .
docker push gitlab.lrz.de:5005/cdb-23/gr4/ms4/kv-server

docker build -t gitlab.lrz.de:5005/cdb-23/gr4/ms4/ecs-server .
docker push gitlab.lrz.de:5005/cdb-23/gr4/ms4/ecs-server

gitlab.lrz.de:5005/cdb-23/gr4/ms4/kv-server

&
python "$PROJECT_PATH/kvserver/kvserver.py" "-a 127.0.0.1" "-p 37959" "-ll INFO" "-d process_data " "-s FIFO" "-c 10" "-b 127.0.0.1:40823"  &
python "$PROJECT_PATH/kvclient/CLI_client.py" "-a 127.0.0.1" "-p 37959"