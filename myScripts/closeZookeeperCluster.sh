zkServer.sh status

parallel-ssh -h servers.txt --outdir /tmp/clusterOutput "source /etc/profile;cd /home/swh/zookeeper-3.7.0/bin;./zkServer.sh status;"

zkServer.sh stop

parallel-ssh -h servers.txt --outdir /tmp/clusterOutput "source /etc/profile;cd /home/swh/zookeeper-3.7.0/bin;./zkServer.sh stop;"

zkServer.sh status

parallel-ssh -h servers.txt --outdir /tmp/clusterOutput "source /etc/profile;cd /home/swh/zookeeper-3.7.0/bin;./zkServer.sh status;"
