docker_path="`pwd`/docker"
unittest_path="`pwd`/.."

cd $docker_path && sh setup.sh
# run unittest
cd $unittest_path && go test .
cd $docker_path && sh teardown.sh
