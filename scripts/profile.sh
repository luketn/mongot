set -ex

# Get the first argument as duration, default to 60 if not provided.
DURATION=${1:-60}

docker exec docker-mongot1-1 "/bin/bash" "/var/lib/mongot-deploy/mongot/bin/profile.sh" $DURATION
mkdir -p ~/.mongot-profiles
docker cp docker-mongot1-1:/profile.html ~/.mongot-profiles/profile-`date "+%Y.%m.%d-%H.%M.%S"`.html
