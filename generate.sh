# !/bin/bash
sudo docker run -v /home/sys_lifj/thrift:/data thrift thrift -o /data/ --gen go /data/cluster.thrift
sudo docker run -v /home/sys_lifj/thrift:/data thrift thrift -o /data/ --gen go /data/sync.thrift
