# ./spark/Dockerfile
FROM apache/spark:3.5.1

# Գործողությունները root-ի տակ, որպեսզի կարողանանք install անել
USER root

# Փոխել pip-ը, տեղադրել numpy և pandas
RUN pip3 install --upgrade pip
RUN pip3 install numpy pandas

# Վերադարձնել spark user-ին
USER spark

# Spark jobs ֆոլդերը որպես working dir
WORKDIR /opt/spark/jobs