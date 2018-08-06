if [ -z "$PURPOSE" ]; then
    echo "PURPOSE must be set"
    exit 1
fi
if [ -z "$CXX" ]; then
    echo "CXX must be set"
    exit 1
fi
if [ -z "$CC" ]; then
    echo "CC must be set"
    exit 1
fi

runcmd(){
    eval $@
    [[ $? != 0 ]] && {
        exit 1
    }
    return 0
}

echo "build combination: PURPOSE=$PURPOSE CXX=$CXX CC=$CC"

rm -rf build && mkdir build && cd build
if ! cmake ..; then
    echo "Fail to generate Makefile by cmake"
    exit 1
fi
make -j4
if [ "$PURPOSE" = "compile" ]; then
    :
elif [ "$PURPOSE" = "test" ]; then
    cd output/bin/ && sh random_test.sh 60
    RETVAL=$?
    if [ $RETVAL -ne 0 ]; then
        echo "Fail to pass random_test"
        exit 1
    fi
else
    echo "Unknown purpose=\"$PURPOSE\""
fi
