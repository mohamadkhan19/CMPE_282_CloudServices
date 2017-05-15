-#!/bin/sh
ssh ubuntu@ip-172-31-15-68 <<EOF
cd /home/ubuntu/praveen/thinktank 
sudo git pull       
sudo npm restart&
 exit       
EOF