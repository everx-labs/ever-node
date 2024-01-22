
set -e
handle_error() {
    echo "An error occurred on $0::$1"
    exit 1
}
trap 'handle_error $LINENO' ERR

function echo_about_running() {

    echo "
    RUNNING NETWORK $1
    "

}

# Start first network "23"
echo_about_running 23
./test_run_net.sh 23 0

# Start second network "48"
echo_about_running 48
./test_run_net.sh 48 10 no-cleanup

# register mesh networks - 48 in 23


# register mesh networks - 23 in 48



