set -ex

docker exec docker-mongot1-1 "/bin/bash" "/var/lib/mongot-deploy/mongot/bin/threaddump.sh"
mkdir -p ~/.mongot-threaddumps
docker cp docker-mongot1-1:/threaddump.txt ~/.mongot-threaddumps/threaddump-`date "+%Y.%m.%d-%H.%M.%S"`.txt

