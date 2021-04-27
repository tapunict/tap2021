#!/usr/bin/env bash
FILE=""
CORES=""
SEPARATOR="w|"
OPTIND=1
# Variable parsing
while getopts "?f:c:s:h" opt; do
    case ${opt} in
        f ) 
            FILE=$OPTARG
            ;;
        c )  
            CORES=$OPTARG
            ;;
        s ) 
            SEPARATOR=$OPTARG
            ;;
        \?) echo "Invalid option : $OPTARG" 1>&2
            echo "Run ./wordcount.sh -h for help"
            exit 1
            ;;
        h) 
            echo "Usage: ./wordcount.sh -f FILEPATH -c CORES -s SEPARATOR"
            echo "Default -s is whitespace (w|)."
            exit 0
            ;;
        esac
    done
shift $((OPTIND-1))

# Variable need check
if [[ -z "$FILE" ]] || [[ -z "$CORES" ]]
then
    echo "Usage: ./wordcount.sh -f FILEPATH -c CORES -s SEPARATOR"
    exit 2
fi

# Stop
docker stop wordcount

# Remove previuos container 
docker container rm wordcount

docker build . -f Dockerfile-WordCount --build-arg TXT=$FILE --build-arg CORES=$CORES --tag alawys:spark
docker run -e SPARK_ACTION=wordcount -e TAP_CODE=$FILE -e CORES=$CORES -e SEPARATOR=$SEPARATOR -p 4040:4040 --name wordcount -it alawys:spark