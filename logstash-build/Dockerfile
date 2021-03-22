FROM docker.elastic.co/logstash/logstash:7.11.1

# Add your logstash plugins setup here
RUN logstash-plugin install logstash-filter-sentimentalizer

# to build with
# docker build . --tag tap:sentimenstash
# and run with 
# docker run --rm -it -v $PWD/pipeline/:/usr/share/logstash/pipeline/ tap:sentimenstash
# make sure that pipeline dir with proper conf is there