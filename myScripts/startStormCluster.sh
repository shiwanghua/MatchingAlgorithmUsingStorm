parallel-ssh -h servers.txt -t 0 --outdir /tmp/clusterOutput "source /etc/profile;tmux new -t storm;storm supervisor"
